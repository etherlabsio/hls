package hls

import (
	"os"
	"strings"

	"github.com/etherlabsio/hls/pkg/ffmpeg"
)

type Transcoder struct {
	execPath string
	options  [][]string
	args     [][]string
	q        map[string]Quality
	images   Image
}

func NewTranscoder(playlist *os.File) (Transcoder, error) {
	t := Transcoder{execPath: "/usr/bin/ffmpeg"}
	t = t.withOptions([]string{"-allowed_extensions", "ALL"}, []string{"-y"}, []string{"-copyts"}, []string{"-i", playlist.Name()})
	t.q = make(map[string]Quality)
	return t, nil
}

func (t Transcoder) WithExecPath(execPath string) Transcoder {
	if execPath != "" && strings.Contains(execPath, "ffmpeg") {
		t.execPath = execPath
	}
	return t
}

func (t Transcoder) WithQuality(q Quality) Transcoder {
	t.q[q.resolution()] = q
	t = t.withArguments(q.settings()...)
	return t
}

func (t Transcoder) WithImage(i Image) Transcoder {
	t.images = i
	t = t.withArguments(i.settings()...)
	return t
}

func (t Transcoder) withOptions(options ...[]string) Transcoder {
	if len(options) != 0 {
		t.options = append(t.options, options...)
	}
	return t
}

func (t Transcoder) withArguments(args ...[]string) Transcoder {
	if len(args) != 0 {
		t.args = append(t.args, args...)
	}
	return t
}

func (t Transcoder) Build() ([]string, error) {
	return ffmpeg.NewBuilder().
		WithExecPath(t.execPath).
		WithOptions(t.options...).
		WithArguments(t.args...).
		Build()
}

func (t Transcoder) Segments() (map[string]*os.File, error) {

	files := make(map[string]*os.File)
	for _, v := range t.q {
		var err error
		files[v.resolution()], err = v.segment()
		if err != nil {
			return nil, err
		}
	}

	return files, nil
}

func (t Transcoder) Images() ([]string, error) {
	return t.images.frames()
}

func (t Transcoder) Close() error {
	for _, v := range t.q {
		err := v.clear()
		if err != nil {
			return err
		}
	}
	return t.images.clear()
}
