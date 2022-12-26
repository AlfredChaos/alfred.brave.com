package database

type Friend struct {
	UID       string `gorm:"type:VARCHAR(36);primary_key;" json:"uid"`
	OwnerUID  string `gorm:"type:VARCHAR(35)" json:"owner_uid"`
	FriendUID string `gorm:"type:VARCHAR(35)" json:"friend_uid"`
}

type FriendFilters struct {
	OwnerUID  *string
	FriendUID *string
}

func (f *Friend) TableName() string {
	return "friends"
}

func (f *Friend) Create() error {
	return DbClient().Create(f).Error
}

func (f *Friend) Delete(uid string) error {
	if f.UID == "" {
		f.UID = uid
	}
	return DbClient().Delete(f).Error
}

func (f *Friend) Update(uid string, values map[string]interface{}) error {
	if f.UID == "" {
		f.UID = uid
	}
	return DbClient().Model(f).Updates(values).Error
}

func (f *Friend) Get(uid string) error {
	return DbClient().First(f, "uid = ?", uid).Error
}

func (f *Friend) List(filters *FriendFilters, friends []Friend) error {
	if filters != nil {
		condition := ""
		if filters.OwnerUID != nil && filters.FriendUID == nil {
			condition = "owner_uid LIKE ?"
			return DbClient().Where(condition, *filters.OwnerUID).Find(&friends).Error
		}
		if filters.OwnerUID == nil && filters.FriendUID != nil {
			condition = "friend_uid LIKE ?"
			return DbClient().Where(condition, *filters.FriendUID).Find(&friends).Error
		}
		if filters.OwnerUID != nil && filters.FriendUID != nil {
			condition = "owner_uid LIKE ? and friend_uid LIKE ?"
			return DbClient().Where(condition, *filters.OwnerUID, *filters.FriendUID).Find(&friends).Error
		}
	}
	return DbClient().Find(&friends).Error
}
