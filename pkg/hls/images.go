package hls

import (
	"io/ioutil"
	"os"
	"sort"
	"strings"

	"github.com/pkg/errors"
)

type Image struct {
	params          FrameParams
	path            string
	keyInfoFilePath string
	segmentFile     *os.File
}

//FrameParams defines parameters for particular frames
type FrameParams struct {
	resolution string
	width      int
	height     int
}

var imageConstraints = map[string]FrameParams{
	"screenshare": FrameParams{
		resolution: "screenshare",
		width:      1280,
		height:     720,
	},
}

func NewImage(resolution string) (Image, error) {
	_, ok := imageConstraints[resolution]
	if !ok {
		return Image{}, errors.New("frame configuration for resolution" + resolution + " is not defined")
	}
	path, err := ioutil.TempDir("", resolution)
	if err != nil {
		return Image{}, err
	}
	q := Image{
		params: imageConstraints[resolution],
		path:   path,
	}
	return q, nil
}

func (i Image) clear() error {
	return os.RemoveAll(i.path)
}

func (i Image) settings() [][]string {
	return [][]string{{"-vf", "fps=3", i.path + "/frame%04d.png"}}
}

func (i Image) imagesList() ([]string, error) {

	var list []string
	files, err := ioutil.ReadDir(i.path)
	if err != nil {
		return list, err
	}

	for _, f := range files {
		fname := f.Name()
		if strings.Contains(fname, "frame") && strings.HasSuffix(fname, ".png") {
			list = append(list, f.Name())
		}
	}
	sort.Strings(list)
	return list, nil
}
func (i Image) imagesPath() (string) {
	return i.path
}

func (i Image) frames() ([]string, error) {
	var files []string
	list, err := i.imagesList()
	if err != nil {
		return nil, err
	}
	for _, k := range list {
		segmentFileName := i.path + "/" + k
		if err != nil {
			return files, err
		}

		files = append(files, segmentFileName)

	}
	return files, nil
}
