package types

type SchemaType int

// JDBC type codes
const (
	Integer SchemaType = 4
	Varchar SchemaType = 12
	Boolean SchemaType = 16
	Long    SchemaType = -5
	Short   SchemaType = 5
	Date    SchemaType = 91
)

type FieldInfo struct {
	Type   SchemaType
	Length int
}
