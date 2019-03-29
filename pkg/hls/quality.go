package hls

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/pkg/errors"
)

type Quality struct {
	params          QualityParams
	path            string
	keyInfoFilePath string
	segmentFile     *os.File
}

//QualityParams defines parameters for particular quality
type QualityParams struct {
	resolution string
	width      int
	height     int
	bitrate    int
	maxrate    int
	bufsize    int
	bandwidth  uint32
	copyVideo  bool
}

var qualityConstraints = map[string]QualityParams{
	"720p": QualityParams{
		resolution: "720p",
		copyVideo:  true,
		width:      1280,
		height:     720,
		bitrate:    2800,
		maxrate:    2996,
		bufsize:    4200,
		bandwidth:  2800000,
	},
	"480p": QualityParams{
		resolution: "480p",
		copyVideo:  false,
		width:      842,
		height:     480,
		bitrate:    1400,
		maxrate:    1498,
		bufsize:    2100,
		bandwidth:  1400000,
	},
	"360p": QualityParams{
		resolution: "360p",
		copyVideo:  false,
		width:      640,
		height:     360,
		bitrate:    800,
		maxrate:    856,
		bufsize:    1200,
		bandwidth:  800000,
	},
	"144p": QualityParams{
		resolution: "144p",
		copyVideo:  false,
		width:      176,
		height:     144,
		bitrate:    300,
		maxrate:    350,
		bufsize:    500,
		bandwidth:  300000,
	},
}

func (q QualityParams) res() string {
	return fmt.Sprintf("%dx%d", q.width, q.height)
}

func (q QualityParams) playlistURI() string {
	return "./" + q.resolution + "/" + q.resolution + ".m3u8"
}

func (q QualityParams) segmentPrefix() string {
	return q.resolution + "_"
}

func hlsOut(dir, keyInfoFile string) [][]string {
	return [][]string{{"-hls_key_info_file", keyInfoFile},
		{"-hls_time", "6"},
		{"-hls_playlist_type", "event"},
		{"-hls_segment_filename", dir + "/out%04d.ts"},
		{dir + "/out.m3u8"}}
}

func NewQuality(resolution string, keyInfoFile *os.File) (Quality, error) {
	_, ok := qualityConstraints[resolution]
	if !ok {
		return Quality{}, errors.New("quality for resolution" + resolution + " is not defined")
	}
	path, err := ioutil.TempDir("", resolution)
	if err != nil {
		return Quality{}, err
	}
	q := Quality{
		params:          qualityConstraints[resolution],
		path:            path,
		keyInfoFilePath: keyInfoFile.Name(),
	}
	return q, nil
}

func (q Quality) resolution() string {
	return q.params.resolution
}

func (q Quality) clear() error {
	return os.RemoveAll(q.path)
}

func (q Quality) settings() [][]string {
	args := [][]string{{"-c:a", "copy"}}
	var params [][]string
	if q.params.copyVideo {
		params = [][]string{{"-c:v", "copy"}}
	} else {
		params = [][]string{{"-vf", "scale=w=" + strconv.Itoa(q.params.width) + ":h=" + strconv.Itoa(q.params.height) + ":force_original_aspect_ratio=decrease"},
			{"-c:a", "copy"},
			{"-c:v", "libx264"},
			{"-preset", "veryfast"},
			{"-profile:v", "main"},
			{"-level", "3.1"},
			{"-crf", "20"},
			{"-sc_threshold", "0"},
			{"-g", "48"},
			{"-keyint_min", "48"},
			{"-tune", "zerolatency"},
			{"-b:v", strconv.Itoa(q.params.bitrate) + "k"},
			{"-maxrate", strconv.Itoa(q.params.maxrate) + "k"},
			{"-bufsize", strconv.Itoa(q.params.bufsize) + "k"}}
	}
	args = append(args, params...)

	return append(args, hlsOut(q.path, q.keyInfoFilePath)...)
}

func (q Quality) segment() (*os.File, error) {
	segmentFileName := q.path + "/out0000.ts"
	var err error
	q.segmentFile, err = os.Open(segmentFileName)
	return q.segmentFile, err
}
