package hls

import (
	"os"

	"github.com/google/go-cloud/blob"

	"github.com/etherlabsio/pkg/commander"
)

type TranscodeEvent struct {
	Bucket                  string   `json:"bucket"`
	Key                     string   `json:"key"`
	DRMKey                  []byte   `json:"drmKey"`
	DRMInitializationVector string   `json:"drmInitializationVector"`
	Qualities               []string `json:"qualities"`
}

type MultirateTranscoder struct {
	bucket    *blob.Bucket
	event     TranscodeEvent
	ffmpegCmd string
	files     map[string]*os.File
	key       string
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

func (m *MultirateTranscoder) Transcode() error {

	transcoder, err := NewTranscoder(m.files["playlist"])
	if err != nil {
		return err
	}
	defer transcoder.Close()

	for _, quality := range m.event.Qualities {
		q, err := NewQuality(quality, m.files["keyInfoFile"])
		if err != nil {
			return err
		}
		transcoder = transcoder.WithQuality(q)
	}

	transcoder = transcoder.WithExecPath(m.ffmpegCmd)

	args, err := transcoder.Build()
	if err != nil {
		return err
	}

	err = commander.Exec(args...)
	if err != nil {
		return err
	}

	segmentFiles, err := transcoder.Segments()
	if err != nil {
		return err
	}

	err = m.Upload(segmentFiles)
	if err != nil {
		return err
	}

	return nil
}

type HLS struct {
	multriateQuality []string
}

func New(multirateQuality []string) (*HLS, error) {

	return &HLS{
		multriateQuality: multirateQuality,
	}, nil
}
