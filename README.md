## 0 File Tree

```
./
├── backend
│   ├── static
│   ├── templates
│   └── backend.py
├── configs
│   ├── nginx.conf
│   ├── storage_nginx.conf
├── db-generation
│   ├── articles
│   ├── bbc_news_texts
│   ├── article.dat
│   ├── genTable_mongoDB10G.py
...
├── .gitignore
├── docker-compose.yml
├── import_data_to_mongo.py
├── import_map_to_mongo.py
├── init.sh
├── README.md
├── slice_table.py
├── task_splitting.md
├── update_file_to_dfs.py
├── update_file_to_dfs.sh
└── utils.py
```



## 1 Initialize

```bash
bash init.sh
```
To pull docker image, you may need to replace the resources with mirror resources.
If the file is uploaded to fdfs correctly, we can access to it by visit http://localhost:8888/group1/M00/00/00/rBIABmcg4nyAbwISAAAJYI_w6cw260.txt