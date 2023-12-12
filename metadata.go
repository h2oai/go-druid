package druid

import (
	_ "embed"
	"strings"
	"time"

	"github.com/h2oai/go-druid/builder/query"
)

type count struct {
	Cnt int `json:"cnt"`
}

// MetadataService is a service that runs druid metadata requests using druid SQL API.
type MetadataService struct {
	client *Client
}

//go:embed sql/datasource_available.sql
var datasourceAvailableQuery string

func fillDataSourceName(in string, ds string) string {
	return strings.Replace(in, "${{ datasource }}", ds, 1)
}

// AwaitDataSourceAvailable awaits for a datasource to be visible in druid table listing.
func (md *MetadataService) AwaitDataSourceAvailable(dataSourceName string) error {
	q := query.
		NewSQL().
		SetQuery(datasourceAvailableQuery).
		SetParameters([]query.SQLParameter{query.NewSQLParameter("VARCHAR", dataSourceName)})
	for range time.Tick(100 * time.Millisecond) {
		var res []count
		_, err := md.client.Query().Execute(q, &res)
		if err != nil {
			return err
		}

		if len(res) >= 1 && res[0].Cnt == 1 {
			break
		}
	}
	return nil
}

//go:embed sql/datasource_records.sql
var datasourceRecordsQuery string

// AwaitRecordsCount awaits for specific recordsCount in a given datasource.
func (md *MetadataService) AwaitRecordsCount(dataSourceName string, recordsCount int) error {
	q := query.NewSQL()
	q.SetQuery(fillDataSourceName(datasourceRecordsQuery, dataSourceName))
	for range time.Tick(100 * time.Millisecond) {
		var res []count
		_, err := md.client.Query().Execute(q, &res)
		if err != nil {
			return err
		}

		if len(res) >= 1 && res[0].Cnt == recordsCount {
			break
		}
	}
	return nil
}
