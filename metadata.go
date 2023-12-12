package druid

import (
	_ "embed"
	"errors"
	"strings"
	"time"

	"github.com/h2oai/go-druid/builder/query"
)

type count struct {
	Cnt int `json:"cnt"`
}

type metadataOptions struct {
	tickerDuration time.Duration
	awaitTimeout   time.Duration
}

type metadataOption func(*metadataOptions)

// MetadataService is a service that runs druid metadata requests using druid SQL API.
type MetadataService struct {
	client         *Client
	tickerDuration time.Duration
	awaitTimeout   time.Duration
}

func NewMetadataService(client *Client, options []metadataOption) *MetadataService {
	opts := &metadataOptions{
		tickerDuration: 500 * time.Millisecond,
		awaitTimeout:   180 * time.Second,
	}
	for _, opt := range options {
		opt(opts)
	}
	md := &MetadataService{
		client:         client,
		tickerDuration: opts.tickerDuration,
		awaitTimeout:   opts.awaitTimeout,
	}
	return md
}

func WithMetadataQueryTicker(duration time.Duration) metadataOption {
	return func(opts *metadataOptions) {
		opts.tickerDuration = duration
	}
}

func WithMetadataQueryTimeout(timeout time.Duration) metadataOption {
	return func(opts *metadataOptions) {
		opts.awaitTimeout = timeout
	}
}

//go:embed sql/datasource_available.sql
var datasourceAvailableQuery string

func fillDataSourceName(in string, ds string) string {
	return strings.Replace(in, "${{ datasource }}", ds, 1)
}

// AwaitDataSourceAvailable awaits for a datasource to be visible in druid table listing.
func (md *MetadataService) AwaitDataSourceAvailable(dataSourceName string) error {
	ticker := time.NewTicker(md.tickerDuration)
	defer ticker.Stop()
	q := query.
		NewSQL().
		SetQuery(datasourceAvailableQuery).
		SetParameters([]query.SQLParameter{query.NewSQLParameter("VARCHAR", dataSourceName)})
	for {
		select {
		case <-ticker.C:
			var res []count
			_, err := md.client.Query().Execute(q, &res)
			if err != nil {
				return err
			}
			if len(res) >= 1 && res[0].Cnt == 1 {
				return nil
			}
		case <-time.After(md.awaitTimeout):
			return errors.New("AwaitDataSourceAvailable timeout")
		}
	}
}

//go:embed sql/datasource_records.sql
var datasourceRecordsQuery string

// AwaitRecordsCount awaits for specific recordsCount in a given datasource.
func (md *MetadataService) AwaitRecordsCount(dataSourceName string, recordsCount int) error {
	ticker := time.NewTicker(md.tickerDuration)
	defer ticker.Stop()
	q := query.NewSQL()
	q.SetQuery(fillDataSourceName(datasourceRecordsQuery, dataSourceName))
	for {
		select {
		case <-ticker.C:
			var res []count
			_, err := md.client.Query().Execute(q, &res)
			if err != nil {
				return err
			}

			if len(res) >= 1 && res[0].Cnt == recordsCount {
				return nil
			}
		case <-time.After(md.awaitTimeout):
			return errors.New("AwaitRecordsCount timeout")
		}
	}
}
