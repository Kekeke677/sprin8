package main

import (
	"database/sql"
	"errors"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS parcel (
			number INTEGER PRIMARY KEY AUTOINCREMENT,
			client INTEGER,
			status TEXT,
			address TEXT,
			created_at TEXT
		)
	`)
	if err != nil {
		panic("Failed to create parcel table: " + err.Error())
	}
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {

	result, err := s.db.Exec(`
		INSERT INTO parcel (client, status, address, created_at)
		VALUES (?, ?, ?, ?)`,
		p.Client, p.Status, p.Address, p.CreatedAt)
	if err != nil {
		return 0, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	return int(id), nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {

	p := Parcel{}
	row := s.db.QueryRow(`
		SELECT number, client, status, address, created_at
		FROM parcel
		WHERE number = ?`, number)
	err := row.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
	if err == sql.ErrNoRows {
		return p, errors.New("parcel not found")
	}
	if err != nil {
		return p, err
	}

	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {

	rows, err := s.db.Query(`
		SELECT number, client, status, address, created_at
		FROM parcel
		WHERE client = ?`, client)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []Parcel
	for rows.Next() {
		var p Parcel
		if err := rows.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt); err != nil {
			return nil, err
		}
		res = append(res, p)
	}

	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {

	_, err := s.db.Exec(`
		UPDATE parcel
		SET status = ?
		WHERE number = ?`, status, number)
	return err
}

func (s ParcelStore) SetAddress(number int, address string) error {

	p, err := s.Get(number)
	if err != nil {
		return err
	}
	if p.Status != ParcelStatusRegistered {
		return errors.New("can only change address for registered parcels")
	}

	_, err = s.db.Exec(`
		UPDATE parcel
		SET address = ?
		WHERE number = ?`, address, number)
	return err
}

func (s ParcelStore) Delete(number int) error {

	p, err := s.Get(number)
	if err != nil {
		return err
	}
	if p.Status != ParcelStatusRegistered {
		return errors.New("can only delete registered parcels")
	}

	_, err = s.db.Exec(`
		DELETE FROM parcel
		WHERE number = ?`, number)
	return err
}
