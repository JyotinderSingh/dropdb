package parse

type CreateViewData struct {
	viewName  string
	queryData *QueryData
}

func NewCreateViewData(viewName string, queryData *QueryData) *CreateViewData {
	return &CreateViewData{
		viewName:  viewName,
		queryData: queryData,
	}
}

func (cvd *CreateViewData) ViewName() string {
	return cvd.viewName
}

func (cvd *CreateViewData) ViewDefinition() string {
	return cvd.queryData.String()
}
