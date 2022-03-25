package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/big"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jinzhu/gorm/dialects/mysql"

	"import_featured_image/models"
)

const (
	username        = "root"
	password        = "root"
	hostname        = "127.0.0.1:3306"
	dbname_trikinet = "trikinet_cms_wp"
	dbname_ds       = "cms"
)

func dsn(dbname_trikinet string) string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, hostname, dbname_trikinet)
}

func dsn_ds(dbname_ds string) string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, hostname, dbname_ds)
}

func GetPosts(db *sql.DB) ([]models.Post, error) {
	query := `select id, post_name, post_title from wp_posts where post_date BETWEEN '2020-01-01' AND '2020-01-31' 
	AND post_type = 'post' AND post_status = 'publish' order by id desc LIMIT 3;`
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return []models.Post{}, err
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return []models.Post{}, err
	}
	defer rows.Close()
	var posts = []models.Post{}
	for rows.Next() {
		var post models.Post
		if err := rows.Scan(&post.Id, &post.Post_title, &post.Post_name); err != nil {
			return []models.Post{}, err
		}
		posts = append(posts, post)
	}
	if err := rows.Err(); err != nil {
		return []models.Post{}, err
	}
	return posts, nil

}

func GetPostsMeta(db *sql.DB, postId int) ([]models.Meta, error) {
	query := `select * FROM wp_postmeta where post_id = ?`
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return []models.Meta{}, err
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx, postId)
	if err != nil {
		return []models.Meta{}, err
	}
	defer rows.Close()
	var post_meta = []models.Meta{}
	for rows.Next() {
		var meta models.Meta
		if err := rows.Scan(&meta.Meta_id, &meta.Post_id, &meta.Meta_key, &meta.Meta_value); err != nil {
			return []models.Meta{}, err
		}
		post_meta = append(post_meta, meta)
	}
	if err := rows.Err(); err != nil {
		return []models.Meta{}, err
	}
	return post_meta, nil

}

func main() {
	db, err := sql.Open("mysql", dsn(""))

	db_ds, err_ds := sql.Open("mysql", dsn_ds(""))

	if err != nil {
		log.Printf("Error %s when opening DB\n", err)
		return
	}
	defer db.Close()

	if err_ds != nil {
		log.Printf("Error %s when opening DB\n", err_ds)
		return
	}
	defer db_ds.Close()

	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	res, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+dbname_trikinet)
	if err != nil {
		log.Printf("Error %s when creating DB\n", err)
		return
	}
	no, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when fetching rows", err)
		return
	}
	log.Printf("rows affected %d\n", no)

	db.Close()
	db, err = sql.Open("mysql", dsn(dbname_trikinet))
	if err != nil {
		log.Printf("Error %s when opening DB", err)
		return
	}
	defer db.Close()

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(time.Minute * 5)

	db_ds, err_ds = sql.Open("mysql", dsn(dbname_ds))
	if err_ds != nil {
		log.Printf("Error %s when opening DB", err_ds)
		return
	}
	defer db_ds.Close()

	db_ds.SetMaxOpenConns(20)
	db_ds.SetMaxIdleConns(20)
	db_ds.SetConnMaxLifetime(time.Minute * 5)

	posts, err := GetPosts(db)
	if err != nil {
		log.Printf("Error %s when selecting post", err)
		return
	}
	for _, post := range posts {
		var postId = big.NewInt(post.Id).String()

		// log.Printf("ID : %q Post Title: %s Post Name: %d", postId, post.Post_name, post.Post_title)

		post_meta, err := GetPostsMeta(db, int(post.Id))

		if err != nil {
			log.Printf("Error %s when selecting post meta", err)
			return
		}

		for _, meta := range post_meta {
			log.Printf("post id : %s, meta key : %q, Meta Value : %p", postId, meta.Meta_key, &meta.Meta_value)
		}

	}
}
