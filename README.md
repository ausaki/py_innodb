一个用 Python 写的读取 InnoDB table space 文件的工具.

阅读 "MySQL 技术内幕: InnoDB 存储引擎" 这本书时, 作者使用了一个读取 InnoDB table space 文件的工具, 去书中提到的 Google code 网页也没有找到对应的代码. 于是网上查了一些资料, 然后自己写了这个小工具.

支持:

- 显示每个页的信息, 例如:
  
  ```
  $ sudo python pyibd.py ~/Workspace/docker/mysql/data/tx_test/test_compact.ibd
  page offset: 00000000, page type: File Space Header
  page offset: 00000001, page type: Insert Buffer Bitmap
  page offset: 00000002, page type: File Segment inode
  page offset: 00000003, page type: SDI Index Page
  page offset: 00000004, page type: B-tree Node, page level: 0000
  page offset: 00000000, page type: Freshly Allocated Page
  page offset: 00000000, page type: Freshly Allocated Page

  Total number of page: 7
  File Space Header: 1
  Insert Buffer Bitmap: 1
  File Segment inode: 1
  SDI Index Page: 1
  B-tree Node: 1
  Freshly Allocated Page: 2
  ```

- 待开发.