package record

type SchemaType int

// type iota enum
const (
	Integer SchemaType = iota
	Varchar
	Boolean
	Long
	Short
	Date
)

type FieldInfo struct {
	fieldType SchemaType
	length    int
}
