package database

import "time"

type User struct {
	ModelBase
	LoginAt  time.Time `gorm:"type:datetime" json:"login_at,omitempty"`
	UserName string    `gorm:"type:VARCHAR(255)" json:"user_name"`
	Email    string    `gorm:"type:VARCHAR(255)" json:"email"`
	Profile  string    `gorm:"type:VARCHAR(255)" json:"profile"`
	Avatar   []byte    `gorm:"type:VARBINARY(255)" json:"avatar"`
	Password []byte    `gorm:"type:VARBINARY(255)" json:"password"`
}

type UserFilters struct {
	UserName     *string
	UserNameList []string
	Email        *string
	EmailList    []string
}

func (u *User) TableName() string {
	return "users"
}

func (u *User) Create() error {
	return DbClient().Create(u).Error
}

func (u *User) Delete(uid string) error {
	if u.UID == "" {
		u.UID = uid
	}
	return DbClient().Delete(u).Error
}

func (u *User) Update(uid string, values map[string]interface{}) error {
	if u.UID == "" {
		u.UID = uid
	}
	return DbClient().Model(u).Updates(values).Error
}

func (u *User) Get(uid string) error {
	return DbClient().First(u, "uid = ?", uid).Error
}

func (u *User) List(filters *UserFilters, users []User) error {
	if filters != nil {
		condition := ""
		if filters.UserName != nil && filters.Email == nil {
			condition = "user_name LIKE ?"
			return DbClient().Where(condition, *filters.UserName).Find(&users).Error
		}
		if filters.UserName == nil && filters.Email != nil {
			condition = "email LIKE ?"
			return DbClient().Where(condition, *filters.Email).Find(&users).Error
		}
		if len(filters.EmailList) != 0 {
			condition = "email IN ?"
			return DbClient().Where(condition, filters.EmailList).Find(&users).Error
		}
		if len(filters.UserNameList) != 0 {
			condition = "user_name IN ?"
			return DbClient().Where(condition, filters.UserNameList).Find(&users).Error
		}
	}
	return DbClient().Find(&users).Error
}
