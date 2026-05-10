package resilix

type Plugin interface {
	Name() string
	Apply(*Options) error
}
