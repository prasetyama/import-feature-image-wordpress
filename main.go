package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
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
	query := `select id, post_title, post_name from wp_posts where post_date BETWEEN '2020-01-01' AND '2020-01-31' 
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

func GetPostbyName(db *sql.DB, post_name string) (int, error) {
	query := `select id from wp_posts where post_name = ?`
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return 0, err
	}
	defer stmt.Close()
	var id int
	row := stmt.QueryRowContext(ctx, post_name)
	if err := row.Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func delete(db *sql.DB, post_id int) error {
	query := "DELETE from wp_postmeta WHERE post_id = ?"
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return err
	}
	defer stmt.Close()
	res, err := stmt.ExecContext(ctx, post_id)
	if err != nil {
		log.Printf("Error %s when delete row into meta post table", err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when finding rows affected", err)
		return err
	}
	log.Printf("%d meta post deleted ", rows)
	return nil
}

func insert(db *sql.DB, meta models.Meta) error {
	query := "INSERT INTO wp_postmeta(post_id, meta_key, meta_value) VALUES (?, ?, ?)"
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return err
	}
	defer stmt.Close()
	res, err := stmt.ExecContext(ctx, meta.Post_id, meta.Meta_key, meta.Meta_value)
	if err != nil {
		log.Printf("Error %s when inserting row into meta post table", err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when finding rows affected", err)
		return err
	}
	log.Printf("%d meta post created ", rows)
	return nil
}

func main() {
	db_trikinet, err := sql.Open("mysql", dsn(""))

	db_ds, err_ds := sql.Open("mysql", dsn_ds(""))

	if err != nil {
		log.Printf("Error %s when opening DB\n", err)
		return
	}
	defer db_trikinet.Close()

	if err_ds != nil {
		log.Printf("Error %s when opening DB\n", err_ds)
		return
	}
	defer db_ds.Close()
	db_trikinet, err = sql.Open("mysql", dsn(dbname_trikinet))
	if err != nil {
		log.Printf("Error %s when opening DB", err)
		return
	}
	defer db_trikinet.Close()

	db_trikinet.SetMaxOpenConns(20)
	db_trikinet.SetMaxIdleConns(20)
	db_trikinet.SetConnMaxLifetime(time.Minute * 5)

	db_ds, err_ds = sql.Open("mysql", dsn(dbname_ds))
	if err_ds != nil {
		log.Printf("Error %s when opening DB", err_ds)
		return
	}
	defer db_ds.Close()

	db_ds.SetMaxOpenConns(20)
	db_ds.SetMaxIdleConns(20)
	db_ds.SetConnMaxLifetime(time.Minute * 5)

	posts, err := GetPosts(db_trikinet)
	if err != nil {
		log.Printf("Error %s when selecting post", err)
		return
	}

	for _, post := range posts {

		// log.Printf("Post Title: %s Post Name: %d", post.Post_name, post.Post_title)

		posts_ds, err := GetPostbyName(db_ds, post.Post_name)

		switch {
		case err == sql.ErrNoRows:
			log.Printf("Post %s not found in DB", post.Post_title)
		case err != nil:
			log.Printf("Encountered err %s when fetching post from DB", err)
		default:
			log.Printf("Post of %s is %d", post.Post_name, posts_ds)
		}

		post_meta_ds, err := GetPostsMeta(db_ds, int(posts_ds))

		if err != nil {
			log.Printf("Error %s when selecting post meta", err)
			return
		}

		for key, meta := range post_meta_ds {
			// log.Printf("post id : %s, meta key : %q, Meta Value : %p", posts_ds, meta.Meta_key, &meta.Meta_value)

			if meta.Meta_key == "_thumbnail_id" {

				var _thumbnail_id_ds, err = strconv.Atoi(post_meta_ds[key].Meta_value)
				thumbnails_ds, err := GetPostsMeta(db_ds, _thumbnail_id_ds)

				if err != nil {
					log.Printf("thumbnail src %s Not found", post.Post_title)
					return
				} else {
					post_meta_trikinet, err := GetPostsMeta(db_trikinet, int(post.Id))

					if err != nil {
						log.Printf("Error %s when selecting post meta", err)
						return
					}

					if len(thumbnails_ds) > 0 {
						log.Printf("Meta Post %s already exist", post.Post_name)

						// delete(db_ds, _thumbnail_id_ds)

						// var _thumbnail_id_trikinet int
						// for key, meta_trikinet := range post_meta_trikinet {
						// 	if meta_trikinet.Meta_key == "_thumbnail_id" {
						// 		_thumbnail_id_trikinet, err = strconv.Atoi(post_meta_trikinet[key].Meta_value)
						// 	}
						// }

						// thumbnails_trikinet, err := GetPostsMeta(db_trikinet, _thumbnail_id_trikinet)

						// for _, thumbnail_trikinet := range thumbnails_trikinet {
						// 	data := models.Meta{
						// 		Post_id:    int64(_thumbnail_id_ds),
						// 		Meta_key:   thumbnail_trikinet.Meta_key,
						// 		Meta_value: thumbnail_trikinet.Meta_value,
						// 	}
						// 	err = insert(db_ds, data)
						// }

						// if err != nil {
						// 	log.Printf("Error %s when selecting post meta", err)
						// 	return
						// }
					} else {
						var _thumbnail_id_trikinet int
						for key, meta_trikinet := range post_meta_trikinet {
							if meta_trikinet.Meta_key == "_thumbnail_id" {
								_thumbnail_id_trikinet, err = strconv.Atoi(post_meta_trikinet[key].Meta_value)
							}
						}

						thumbnails_trikinet, err := GetPostsMeta(db_trikinet, _thumbnail_id_trikinet)

						for _, thumbnail_trikinet := range thumbnails_trikinet {
							data := models.Meta{
								Post_id:    int64(_thumbnail_id_ds),
								Meta_key:   thumbnail_trikinet.Meta_key,
								Meta_value: thumbnail_trikinet.Meta_value,
							}
							err = insert(db_ds, data)
						}

						if err != nil {
							log.Printf("Error %s when selecting post meta", err)
							return
						}
					}
				}
			}
		}

	}
}
