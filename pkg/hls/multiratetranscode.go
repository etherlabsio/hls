package hls

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/etherlabsio/m3u8"
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
	plist, err := m3u8.NewMediaPlaylist(1, 1)
	if err != nil {
		return err
	}
	const playlistVersion = 3
	const playlistTargetDuration = 6.0
	const encryptMethod = "AES-128"
	plist.TargetDuration = playlistTargetDuration
	plist.SetVersion(playlistVersion)
	plist.MediaType = m3u8.VOD
	plist.Key = &m3u8.Key{
		IV:     DRMInitializationVector,
		URI:    m.files["keyFile"].Name(),
		Method: encryptMethod,
	}
	plist.AppendSegment(&m3u8.MediaSegment{URI: m.files["segment"].Name(), Duration: playlistTargetDuration})
	plist.Closed = true
	_, err = m.files["playlist"].Write(plist.Encode().Bytes())
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

func (m *MultirateTranscoder) Upload(segments map[string]*os.File) error {

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

	for k, f := range m.files {
		n := f.Name()
		f.Close()
		os.Remove(n)
		delete(m.files, k)
	}
}
