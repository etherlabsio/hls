package hls

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/AlekSi/pointer"
	"github.com/etherlabsio/go-m3u8/m3u8"
)

const BUFFERSIZE = 10240

func copyFile(src io.Reader, dst io.Writer) error {
	buf := make([]byte, BUFFERSIZE)
	for {
		n, err := src.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		if _, err := dst.Write(buf[:n]); err != nil {
			return err
		}
	}
	return nil
}

func (m *MultirateTranscoder) generatePlaylistForSegment(DRMInitializationVector string) error {
	const (
		playlistVersion        = 3
		playlistTargetDuration = 6.0
		encryptMethod          = "AES-128"
		mediaType              = "VOD"
	)

	playlist := m3u8.NewPlaylist()
	playlist.Version = pointer.ToInt(playlistVersion)
	playlist.Target = playlistTargetDuration
	playlist.Type = pointer.ToString(mediaType)
	playlist.AppendItem(&m3u8.KeyItem{
		Encryptable: &m3u8.Encryptable{
			IV:     pointer.ToString(DRMInitializationVector),
			URI:    pointer.ToString(m.files["keyFile"].Name()),
			Method: encryptMethod,
		},
	})
	playlist.AppendItem(&m3u8.SegmentItem{
		Segment:  m.files["segment"].Name(),
		Duration: playlistTargetDuration,
	})
	playlist.Live = false
	_, err := m.files["playlist"].Write([]byte(playlist.String()))
	if err != nil {
		return err
	}

	return nil
}

func (m *MultirateTranscoder) createTempFiles(key string, DRMKey []byte, DRMInitializationVector string) error {

	m.key = key
	ctx := context.Background()
	reader, err := m.bucket.NewReader(ctx, key, nil)
	if err != nil {
		return err
	}
	defer reader.Close()

	m.files["segment"], err = ioutil.TempFile("", "segment*.ts")
	if err != nil {
		return err
	}

	err = copyFile(reader, m.files["segment"])
	if err != nil {
		return err
	}

	m.files["keyFile"], err = ioutil.TempFile("", "key*.pem")
	if err != nil {
		return err
	}

	_, err = m.files["keyFile"].Write(DRMKey)
	if err != nil {
		return err
	}

	const defaultKeyURI = "dummy"

	/* key info file*/
	infoFileContent := []byte(defaultKeyURI + "\n" + m.files["keyFile"].Name() + "\n" + DRMInitializationVector + "\n")
	m.files["keyInfoFile"], err = ioutil.TempFile("", "keyinfofile*.txt")
	if err != nil {
		return err
	}

	_, err = m.files["keyInfoFile"].Write(infoFileContent)
	if err != nil {
		return err
	}

	m.files["playlist"], err = ioutil.TempFile("", "in*.m3u8")
	return err
}

func (m *MultirateTranscoder) upload(segments map[string]*os.File) error {

	p := path.Dir(m.key)
	fn := path.Base(m.key)
	filename := strings.TrimSuffix(fn, path.Ext(fn))
	index := strings.TrimPrefix(filename, "out")

	ctx := context.Background()
	for resolution, segment := range segments {
		segmentKey := p + "/" + resolution + "/" + resolution + "_" + index + ".ts"
		segmentWriter, err := m.bucket.NewWriter(ctx, segmentKey, nil)
		if err != nil {
			return err
		}
		err = copyFile(segment, segmentWriter)
		if err != nil {
			return err
		}
		segmentWriter.Close()
	}
	return nil
}

func (m *MultirateTranscoder) Close() {

	m.transcoder.Close()

	for k, f := range m.files {
		n := f.Name()
		f.Close()
		os.Remove(n)
		delete(m.files, k)
	}
}
