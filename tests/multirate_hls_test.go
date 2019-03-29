package tests

import (
	"context"
	"os"
	"testing"

	"github.com/google/go-cloud/blob/s3blob"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/etherlabsio/hls/pkg/hls"
)

func makeAWSSession(region string) *session.Session {
	c := &aws.Config{
		// Either hard-code the region or use AWS_REGION.
		Region: aws.String(region),
	}
	s := session.Must(session.NewSession(c))
	return s
}

func Test_multirateHLSUpdatePlaylist(t *testing.T) {
	region := os.Getenv("AWS_REGION")
	tests := []struct {
		name        string
		Region      string
		Bucket      string
		KeyPlaylist string
		wantErr     bool
	}{

		{
			"Basic functionality",
			region,
			os.Getenv("AWS_BUCKET"),
			"recordings/hls-test/master1.m3u8",
			false,
		},
	}
	AWSSession := makeAWSSession(region)
	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := hls.New([]string{"720p", "480p", "360p"})
			if err != nil {
				t.Errorf("error while creating NewMultirateHLS error %v", err)
			}

			bucket, _ := s3blob.OpenBucket(ctx, tt.Bucket, AWSSession, nil)

			err = m.GenerateMultiratePlaylist(ctx, bucket, tt.KeyPlaylist)
			if (err != nil) != tt.wantErr {
				t.Errorf("error with Transcode error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
