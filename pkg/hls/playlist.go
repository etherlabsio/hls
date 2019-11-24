package hls

import (
	"context"
	"io"
	"path"
	"strings"

	"github.com/AlekSi/pointer"
	"github.com/etherlabsio/go-m3u8/m3u8"
	"gocloud.dev/blob"
	"github.com/pkg/errors"
)

type playlist struct {
	p   *m3u8.Playlist
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

func (h *HLS) fetchPlaylist(ctx context.Context, bucket *blob.Bucket, playlistPath string) (*m3u8.Playlist, error) {

	r, err := bucket.NewReader(ctx, playlistPath, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "key %s", playlistPath)
	}
	defer r.Close()

	return m3u8.Read(r)
}

func (h *HLS) fetchPlaylists(ctx context.Context, bucket *blob.Bucket, keyPlaylist string) (map[string]playlist, error) {
	p := make(map[string]playlist)

	m, err := h.fetchPlaylist(ctx, bucket, keyPlaylist)
	if err != nil {
		return p, errors.Wrap(err, "failed to fetch master playlist")
	}
	if !m.IsMaster() {
		return p, errors.New("received media playlist instead of master playlist")
	}

	p["master"] = playlist{p: m, uri: keyPlaylist}

	var uri string
	plistCount := 0
	for _, i := range m.Items {
		if pi, ok := i.(*m3u8.PlaylistItem); ok {

			plistCount++
			uri = path.Dir(keyPlaylist) + "/" + pi.URI
			uri = path.Clean(uri)
		}
	}

	if plistCount > 1 {
		return p, errors.New("master playlist contains more than 1 sub playlists")
	}
	if plistCount == 0 {
		return p, errors.New("master playlist doesnt contain sub playlist")
	}

	subPlaylist, err := h.fetchPlaylist(ctx, bucket, uri)
	p["sub"] = playlist{p: subPlaylist, uri: uri}

	return p, err
}

func (h *HLS) isValidMultiratePlaylist(ctx context.Context,
	bucket *blob.Bucket,
	playlistDirectory string,
	q map[string]QualityParams) (bool, error) {

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

	q := make(map[string]QualityParams)
	for _, quality := range h.multriateQuality {
		q[quality] = qualityConstraints[quality]
	}

	p, err := h.fetchPlaylists(ctx, bucket, playlistURI)
	if err != nil {
		return errors.Wrap(err, "failed to fetch the playlists")
	}
	ok, err := h.isValidMultiratePlaylist(ctx, bucket, path.Dir(playlistURI), q)
	if !ok {
		return errors.Wrap(err, "failed to validate playlist for multirate.")
	}

	playlists, err := h.generatePlaylist(q, p["sub"].p)
	if err != nil {
		return errors.Wrap(err, "failed to generate playlists")
	}

	return errors.WithMessage(h.uploadMultiratePlaylists(ctx, q, playlists, bucket, playlistURI), "error while trying to upload multirate playlists")
}

func (h *HLS) generatePlaylist(q map[string]QualityParams, subPlaylist *m3u8.Playlist) (map[string]*m3u8.Playlist, error) {

	m := m3u8.NewPlaylist()
	m.Master = pointer.ToBool(true)

	type plistInfo struct {
		plist  *m3u8.Playlist
		params QualityParams
	}

	plists := make(map[string]plistInfo)

	var keyinfo *m3u8.KeyItem
	for _, i := range subPlaylist.Items {
		if ki, ok := i.(*m3u8.KeyItem); ok {
			keyinfo = ki
			break
		}
	}

	for quality, qparams := range q {
		plist := m3u8.NewPlaylist()
		plist.Target = subPlaylist.Target
		plist.Version = subPlaylist.Version
		plist.Type = subPlaylist.Type
		if keyinfo != nil {
			plist.AppendItem(keyinfo)
		}

		m.AppendItem(&m3u8.PlaylistItem{
			Bandwidth: int(qparams.bandwidth),
			Resolution: &m3u8.Resolution{
				Width:  qparams.width,
				Height: qparams.height,
			},
			URI: qparams.playlistURI(),
		})

		plists[quality] = plistInfo{plist: plist, params: qparams}

	}
	for _, segment := range subPlaylist.Segments() {
		if segment != nil {
			for quality := range plists {
				seg := *segment
				seg.Segment = strings.Replace(seg.Segment, "out", plists[quality].params.segmentPrefix(), 1)
				plists[quality].plist.AppendItem(&seg)
			}
		}
	}
	for _, p := range plists {
		p.plist.Live = false
	}

	result := make(map[string]*m3u8.Playlist)
	result["master"] = m

	for quality, p := range plists {
		result[quality] = p.plist
	}

	return result, nil
}

func (h *HLS) uploadMultiratePlaylists(ctx context.Context, q map[string]QualityParams,
	playlists map[string]*m3u8.Playlist,
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
		_, err = wr.Write([]byte(p.String()))
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
	_, err = wr.Write([]byte(p.String()))
	if err != nil {
		return errors.Wrapf(err, "failed to write to the file %s", playlistURI)
	}

	return nil
}
