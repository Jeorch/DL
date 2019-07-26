package PhProxy

type PhProxy interface {
	Create(args map[string]interface{}) (data map[string]interface{}, err error)
	Update(args map[string]interface{}) (data map[string]interface{}, err error)
	Read(args map[string]interface{}) (data map[string]interface{}, err error)
	Delete(args map[string]interface{}) (data map[string]interface{}, err error)

	Format(data map[string]interface{}) (resp interface{}, err error)
}