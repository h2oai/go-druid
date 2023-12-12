package druid

import (
	"context"
	_ "embed"
	"errors"
	"strings"
	"time"

	"github.com/h2oai/go-druid/builder/query"
)

type count struct {
	Cnt int `json:"cnt"`
}

// MetadataService is a service that runs druid metadata requests using druid SQL API.
type MetadataService struct {
	client             *Client
	tickerMilliseconds time.Duration
	waitTimeoutSeconds int
}

//go:embed sql/datasource_available.sql
var datasourceAvailableQuery string

func fillDataSourceName(in string, ds string) string {
	return strings.Replace(in, "${{ datasource }}", ds, 1)
}

// AwaitDataSourceAvailable awaits for a datasource to be visible in druid table listing.
func (md *MetadataService) AwaitDataSourceAvailable(dataSourceName string) error {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(time.Duration(md.waitTimeoutSeconds) * time.Second)
		cancel()
	}()
	ticker := time.NewTicker(md.tickerMilliseconds * time.Millisecond)
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
				break
			}
		case <-ctx.Done():
			return errors.New("AwaitDataSourceAvailable timeout")
		}
	}
}

//go:embed sql/datasource_records.sql
var datasourceRecordsQuery string

// AwaitRecordsCount awaits for specific recordsCount in a given datasource.
func (md *MetadataService) AwaitRecordsCount(dataSourceName string, recordsCount int) error {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(time.Duration(md.waitTimeoutSeconds) * time.Second)
		cancel()
	}()
	ticker := time.NewTicker(md.tickerMilliseconds * time.Millisecond)

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
				break
			}
		case <-ctx.Done():
			return errors.New("AwaitRecordsCount timeout")
		}
	}
}
