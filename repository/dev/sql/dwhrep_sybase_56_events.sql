create table UserPreferences(
	ID integer primary key default autoincrement,
	USERNAME varchar(20) not null,
	VERSION integer,
	SETTINGS text
)

