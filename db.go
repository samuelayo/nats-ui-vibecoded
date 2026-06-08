package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

type ProfileStore struct {
	db *sql.DB
}

type ConnectionProfile struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	URL       string `json:"url"`
	Username  string `json:"username"`
	Password  string `json:"password,omitempty"`
	Token     string `json:"token,omitempty"`
	CredsPath string `json:"credsPath"`
}

type ProfileView struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	URL         string `json:"url"`
	Username    string `json:"username"`
	CredsPath   string `json:"credsPath"`
	HasPassword bool   `json:"hasPassword"`
	HasToken    bool   `json:"hasToken"`
}

func NewProfileStore() (*ProfileStore, error) {
	appDir, err := getAppDir()
	if err != nil {
		return nil, err
	}
	dbPath := filepath.Join(appDir, "nats-ui.db")
	db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS profiles (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		url TEXT NOT NULL,
		username TEXT NOT NULL DEFAULT '',
		password TEXT NOT NULL DEFAULT '',
		token TEXT NOT NULL DEFAULT '',
		creds_path TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL DEFAULT (datetime('now')),
		updated_at TEXT NOT NULL DEFAULT (datetime('now'))
	)`); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}
	return &ProfileStore{db: db}, nil
}

func getAppDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(home, ".nats-ui")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	return dir, nil
}

func (s *ProfileStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *ProfileStore) List() ([]ConnectionProfile, error) {
	rows, err := s.db.Query("SELECT id, name, url, username, password, token, creds_path FROM profiles ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var profiles []ConnectionProfile
	for rows.Next() {
		var p ConnectionProfile
		if err := rows.Scan(&p.ID, &p.Name, &p.URL, &p.Username, &p.Password, &p.Token, &p.CredsPath); err != nil {
			return nil, err
		}
		profiles = append(profiles, p)
	}
	return profiles, rows.Err()
}

func (s *ProfileStore) ListViews() ([]ProfileView, error) {
	profiles, err := s.List()
	if err != nil {
		return nil, err
	}
	views := make([]ProfileView, 0, len(profiles))
	for _, p := range profiles {
		views = append(views, ProfileView{
			ID: p.ID, Name: p.Name, URL: p.URL, Username: p.Username, CredsPath: p.CredsPath,
			HasPassword: p.Password != "", HasToken: p.Token != "",
		})
	}
	return views, nil
}

func (s *ProfileStore) Get(id int64) (*ConnectionProfile, error) {
	row := s.db.QueryRow("SELECT id, name, url, username, password, token, creds_path FROM profiles WHERE id = ?", id)
	var p ConnectionProfile
	if err := row.Scan(&p.ID, &p.Name, &p.URL, &p.Username, &p.Password, &p.Token, &p.CredsPath); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &p, nil
}

func (s *ProfileStore) Save(p ConnectionProfile) (int64, error) {
	if p.Password != "" && !isObfuscated(p.Password) {
		p.Password = obfuscate(p.Password)
	}
	res, err := s.db.Exec("INSERT INTO profiles (name, url, username, password, token, creds_path) VALUES (?, ?, ?, ?, ?, ?)",
		p.Name, p.URL, p.Username, p.Password, p.Token, p.CredsPath)
	if err != nil {
		return 0, fmt.Errorf("save profile: %w", err)
	}
	return res.LastInsertId()
}

func (s *ProfileStore) Delete(id int64) error {
	_, err := s.db.Exec("DELETE FROM profiles WHERE id = ?", id)
	return err
}

func profileSecretValue(v string) string {
	if isObfuscated(v) {
		return deobfuscate(v)
	}
	return v
}

func obfuscate(s string) string {
	key := byte(0xAB)
	b := []byte(s)
	for i := range b {
		b[i] ^= key
		key = key*17 + byte(i)
	}
	return fmt.Sprintf("%x", b)
}

func deobfuscate(hex string) string {
	b := make([]byte, len(hex)/2)
	for i := 0; i < len(hex); i += 2 {
		fmt.Sscanf(hex[i:i+2], "%x", &b[i/2])
	}
	key := byte(0xAB)
	for i := range b {
		b[i] ^= key
		key = key*17 + byte(i)
	}
	return string(b)
}

func isObfuscated(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return len(s) >= 2
}
