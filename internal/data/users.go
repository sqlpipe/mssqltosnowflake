package data

var AnonymousUser = &User{}

type User struct {
}

func (u *User) IsAnonymous() bool {
	return u == AnonymousUser
}
