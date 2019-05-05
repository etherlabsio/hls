package hls

import (
	"os"

	"github.com/google/go-cloud/blob"
	"github.com/pkg/errors"
)

type TranscodeEvent struct {
	Bucket                  string   `json:"bucket"`
	Key                     string   `json:"key"`
	DRMKey                  []byte   `json:"drmKey"`
	DRMInitializationVector string   `json:"drmInitializationVector"`
	Qualities               []string `json:"qualities"`
	ExtractImages           bool     `json:"extractImages"`
}

type MultirateTranscoder struct {
	bucket     *blob.Bucket
	event      TranscodeEvent
	ffmpegCmd  string
	files      map[string]*os.File
	key        string
	transcoder Transcoder
}

func NewMultirateTranscoder(bucket *blob.Bucket, event TranscodeEvent, ffmpegCmd string) (*MultirateTranscoder, error) {
	m := MultirateTranscoder{
		bucket:    bucket,
		event:     event,
		ffmpegCmd: ffmpegCmd,
		files:     make(map[string]*os.File),
	}
	err := m.createTempFiles(m.event.Key, m.event.DRMKey, m.event.DRMInitializationVector)
	if err != nil {
		return &m, err
	}
	err = m.generatePlaylistForSegment(m.event.DRMInitializationVector)
	if err != nil {
		return &m, err
	}
	return &m, nil
}

func (m *MultirateTranscoder) GenerateCommand() ([]string, error) {

	transcoder, err := NewTranscoder(m.files["playlist"])
	if err != nil {
		return nil, err
	}
	m.transcoder = transcoder

	for _, quality := range m.event.Qualities {
		q, err := NewQuality(quality, m.files["keyInfoFile"])
		if err != nil {
			return nil, err
		}
		m.transcoder = m.transcoder.WithQuality(q)
	}
	if m.event.ExtractImages {
		i, err := NewImage("screenshare")
		if err != nil {
			return nil, err
		}
		m.transcoder = m.transcoder.WithImage(&i)
	}

	m.transcoder = m.transcoder.WithExecPath(m.ffmpegCmd)

	return m.transcoder.Build()
}

func (m *MultirateTranscoder) Upload() error {

	segmentFiles, err := m.transcoder.Segments()
	if err != nil {
		return err
	}
	return m.upload(segmentFiles)
}

func (m *MultirateTranscoder) ImagesPath() (string, error) {
	if m.event.ExtractImages {
		return m.transcoder.ImagesPath()
	}
	return "", errors.New("extract images is disabled.")
}

type HLS struct {
	multriateQuality []string
}

func New(multirateQuality []string) (*HLS, error) {

	return &HLS{
		multriateQuality: multirateQuality,
	}, nil
}
