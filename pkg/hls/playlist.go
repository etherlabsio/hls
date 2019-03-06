package hls

import (
	"context"
	"io"
	"path"
	"strings"

	"github.com/etherlabsio/m3u8"
	"github.com/google/go-cloud/blob"
	"github.com/pkg/errors"
)

type playlist struct {
	p   m3u8.Playlist
	uri string
}

func ListDir(ctx context.Context, bucket *blob.Bucket, prefix string) ([]string, error) {
	var filelist []string
	iter := bucket.List(&blob.ListOptions{Prefix: prefix})
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		filelist = append(filelist, obj.Key)
	}
	return filelist, nil
}

func (h *HLS) fetchPlaylist(ctx context.Context, bucket *blob.Bucket, playlistPath string) (m3u8.Playlist, m3u8.ListType, error) {

	r, err := bucket.NewReader(ctx, playlistPath, nil)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "key %s", playlistPath)
	}
	defer r.Close()

	return m3u8.DecodeFrom(r, false)
}

func (h *HLS) fetchPlaylists(ctx context.Context, bucket *blob.Bucket, keyPlaylist string) (map[string]playlist, error) {
	p := make(map[string]playlist)

	m, listType, err := h.fetchPlaylist(ctx, bucket, keyPlaylist)
	if err != nil {
		return p, err
	}
	if listType != m3u8.MASTER {
		return p, err
	}

	p["master"] = playlist{p: m, uri: keyPlaylist}

	masterPlaylist := m.(*m3u8.MasterPlaylist)
	if len(masterPlaylist.Variants) > 1 {
		return p, errors.New("master playlist contains more than 1 sub playlists")
	}
	if len(masterPlaylist.Variants) == 0 {
		return p, errors.New("master playlist doesnt contain sub playlist")
	}

	uri := path.Dir(keyPlaylist) + "/" + masterPlaylist.Variants[0].URI
	uri = path.Clean(uri)

	subPlaylist, listType, err := h.fetchPlaylist(ctx, bucket, uri)
	p["sub"] = playlist{p: subPlaylist, uri: uri}

	return p, err
}

func (h *HLS) isValidMultiratePlaylist(ctx context.Context,
	bucket *blob.Bucket,
	p m3u8.Playlist,
	playlistDirectory string,
	q map[string]qualityParams) (bool, error) {

	filelist, err := ListDir(ctx, bucket, playlistDirectory)
	if err != nil {
		return false, err
	}
	allPaths := strings.Join(filelist, ":")

	var filelisterr error
	for _, f := range filelist {
		if strings.Contains(f, playlistDirectory+"/out") {
			for _, quality := range q {
				r := "/" + quality.res() + "/" + quality.segmentPrefix()
				nstr := strings.Replace(f, "/out", r, 1)
				if !strings.Contains(allPaths, nstr) {
					filelisterr = errors.Wrap(filelisterr, "missing file "+nstr)
				}
			}
		}
	}
	if filelisterr != nil {
		return false, filelisterr
	}

	return true, nil
}

func (h *HLS) GenerateMultiratePlaylist(ctx context.Context, bucket *blob.Bucket, playlistURI string) error {

	q := make(map[string]qualityParams)
	for _, quality := range h.multriateQuality {
		q[quality] = qualityConsts[quality]
	}

	p, err := h.fetchPlaylists(ctx, bucket, playlistURI)
	if err != nil {
		return errors.Wrap(err, "failed to fetch the playlists")
	}
	ok, err := h.isValidMultiratePlaylist(ctx, bucket, p["sub"].p, path.Dir(playlistURI), q)
	if !ok {
		return errors.Wrap(err, "failed to validate playlist for multirate.")
	}

	playlists, err := h.generatePlaylist(q, p["sub"].p.(*m3u8.MediaPlaylist))
	if err != nil {
		return errors.Wrap(err, "failed to generate playlists")
	}

	return errors.WithMessage(h.uploadMultiratePlaylists(ctx, q, playlists, bucket, playlistURI), "error while trying to upload multirate playlists")
}

func (h *HLS) generatePlaylist(q map[string]qualityParams, subPlaylist *m3u8.MediaPlaylist) (map[string]m3u8.Playlist, error) {

	m := m3u8.NewMasterPlaylist()

	numSegments := subPlaylist.Count()

	type plistInfo struct {
		plist  *m3u8.MediaPlaylist
		params qualityParams
	}

	plists := make(map[string]plistInfo)

	for quality, qparams := range q {
		params := m3u8.VariantParams{
			Bandwidth:  qparams.bandwidth,
			Resolution: qparams.res(),
		}
		plist, err := m3u8.NewMediaPlaylist(numSegments, 2*numSegments)
		if err != nil {
			return nil, err
		}
		plists[quality] = plistInfo{plist: plist, params: qparams}
		plist.TargetDuration = subPlaylist.TargetDuration
		plist.SetVersion(subPlaylist.Version())
		plist.MediaType = subPlaylist.MediaType

		m.Append(qparams.playlistURI(), plist, params)
	}
	for _, segment := range subPlaylist.Segments {
		if segment != nil {
			for quality := range plists {
				seg := *segment
				seg.URI = strings.Replace(seg.URI, "out", plists[quality].params.segmentPrefix(), 1)
				err := plists[quality].plist.AppendSegment(&seg)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	for _, p := range plists {
		p.plist.Close()
	}

	result := make(map[string]m3u8.Playlist)
	result["master"] = m

	for quality, p := range plists {
		result[quality] = p.plist
	}

	return result, nil
}

func (h *HLS) uploadMultiratePlaylists(ctx context.Context, q map[string]qualityParams,
	playlists map[string]m3u8.Playlist,
	bucket *blob.Bucket, playlistURI string) error {
	for k, v := range q {
		p, ok := playlists[k]
		if !ok {
			return errors.New("missing playlist for quality " + k + " while generating multirate playlists")
		}
		uri := path.Dir(playlistURI) + "/" + v.playlistURI()
		wr, err := bucket.NewWriter(ctx, uri, &blob.WriterOptions{BufferSize: 0, ContentType: "application/vnd.apple.mpegurl"})
		if err != nil {
			return errors.Wrapf(err, "failed to open the file %s for writing", uri)
		}
		defer wr.Close()
		_, err = wr.Write(p.Encode().Bytes())
		if err != nil {
			return errors.Wrapf(err, "failed to write to the file %s", uri)
		}
	}
	p, ok := playlists["master"]
	if !ok {
		return errors.New("missing master playlist while generating multirate playlists")
	}
	wr, err := bucket.NewWriter(ctx, playlistURI, &blob.WriterOptions{BufferSize: 0, ContentType: "application/vnd.apple.mpegurl"})
	if err != nil {
		return errors.Wrapf(err, "failed to open the file %s for writing", playlistURI)
	}
	defer wr.Close()
	_, err = wr.Write(p.Encode().Bytes())
	if err != nil {
		return errors.Wrapf(err, "failed to write to the file %s", playlistURI)
	}

	return nil
}
