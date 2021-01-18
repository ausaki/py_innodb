
import os

# page size
PAGE_SIZE = 1 << 14

# Start of the data on the page
FIL_PAGE_DATA = 38

# page offset inside space
FIL_PAGE_OFFSET = 4 
# File page type 
FIL_PAGE_TYPE = 24  

# Types of an undo log segment */
TRX_UNDO_INSERT = 1
TRX_UNDO_UPDATE = 2

# On a page of any file segment, data may be put starting from this offset
FSEG_PAGE_DATA = FIL_PAGE_DATA

# The offset of the undo log page header on pages of the undo log
TRX_UNDO_PAGE_HDR = FSEG_PAGE_DATA

PAGE_LEVEL = 26  # level of the node in an index tree; the leaf level is the level 0 */

# page types. for more details see https://github.com/mysql/mysql-server/blob/ee4455a33b10f1b1886044322e4893f587b319ed/storage/innobase/include/fil0fil.h
# Freshly allocated page
PAGE_TYPE_ALLOCATED = 0x0000
# Undo log page
PAGE_TYPE_UNDO_LOG = 0x0002
# Index node
PAGE_TYPE_INODE = 0x0003
# Insert buffer free list
PAGE_TYPE_IBUF_FREE_LIST = 0x0004
# Insert buffer bitmap
PAGE_TYPE_IBUF_BITMAP = 0x0005
# System page
PAGE_TYPE_SYS = 0x0006
# Transaction system data
PAGE_TYPE_TRX_SYS = 0x0007
# File space header
PAGE_TYPE_FSP_HDR = 0x0008
# Extent descriptor page
PAGE_TYPE_XDES = 0x0009
# Uncompressed BLOB page 
PAGE_TYPE_BLOB = 0x000a
# First compressed BLOB page 
PAGE_TYPE_ZBLOB = 0x000b
# Subsequent compressed BLOB page
PAGE_TYPE_ZBLOB2 = 0x000c
# Data pages of uncompressed LOB
PAGE_TYPE_LOB_DATA = 0x17
# The first page of an uncompressed LOB
PAGE_TYPE_LOB_FIRST = 0x18
# Tablespace SDI Index page
PAGE_TYPE_SDI = 0x45bd
# R-tree node
PAGE_TYPE_RTREE = 0x45be
# B-tree node
PAGE_TYPE_BTREE_NODE = 0x45bf



PAGE_TYPES = {
    PAGE_TYPE_ALLOCATED: "Freshly Allocated Page",
    PAGE_TYPE_UNDO_LOG: "Undo Log Page",
    PAGE_TYPE_INODE: "File Segment inode",
    PAGE_TYPE_IBUF_FREE_LIST: "Insert Buffer Free List",
    PAGE_TYPE_IBUF_BITMAP: "Insert Buffer Bitmap",
    PAGE_TYPE_SYS: "System Page",
    PAGE_TYPE_TRX_SYS: "Transaction system Page",
    PAGE_TYPE_FSP_HDR: "File Space Header",
    PAGE_TYPE_XDES: "extend description page",
    PAGE_TYPE_BLOB: "Uncompressed BLOB Page",
    PAGE_TYPE_ZBLOB: "1st compressed BLOB Page",
    PAGE_TYPE_ZBLOB2: "Subsequent compressed BLOB Page",
    PAGE_TYPE_BTREE_NODE: "B-tree Node",
    PAGE_TYPE_LOB_DATA: 'Uncompressed LOB Page',
    PAGE_TYPE_LOB_FIRST: '1st Uncompressed LOB Page',
    PAGE_TYPE_SDI: 'SDI Index Page'
}

def page_type_str(page_type):
    return PAGE_TYPES.get(page_type, 'UNKNOWN({:04x})'.format(page_type))


INNODB_PAGE_DIRECTION = {
    "0000": "Unknown(0x0000)",
    "0001": "Page Left",
    "0002": "Page Right",
    "0003": "Page Same Rec",
    "0004": "Page Same Page",
    "0005": "Page No Direction",
    "ffff": "Unkown2(0xffff)",
}


class InnoDB:

    def __init__(self, ibd_file):
        self._file = ibd_file
    
    def parse(self):
        with open(self._file, 'rb') as fp:
            fsize = os.fstat(fp.fileno()).st_size
            npages = fsize // PAGE_SIZE
            for i in range(npages):
                page = fp.read(PAGE_SIZE)
                page = Page(page)
                yield page
        
    def output(self):
        pages = self.parse()
        npages = 0
        g = {}
        for page in pages:
            print(page)
            g[page.type_str] = g.get(page.type_str, 0) + 1
            npages += 1
        print()
        print('Total number of page: {}'.format(npages))
        for k, v in g.items():
            print('{}: {}'.format(k, v))


class Page:
    def __init__(self, page):
        self._page = page
        self.parse()

    def parse(self):
        self.offset = int.from_bytes(self._page[FIL_PAGE_OFFSET: FIL_PAGE_OFFSET + 4], 'big', signed=False)
        self.type = int.from_bytes(self._page[FIL_PAGE_TYPE: FIL_PAGE_TYPE + 2], 'big', signed=False)
        self.level = int.from_bytes(self._page[FIL_PAGE_DATA + PAGE_LEVEL: FIL_PAGE_DATA + PAGE_LEVEL + 2], 'big', signed=False)

    @property
    def type_str(self):
        return page_type_str(self.type)

    def __str__(self) -> str:
        s = 'page offset: {:08x}, page type: {}'.format(self.offset, self.type_str)
        if self.type == PAGE_TYPE_BTREE_NODE:
            s += ', page level: {:04x}'.format(self.level)
        return s

def main(ibd_file):
    innodb = InnoDB(ibd_file)
    # pages = innodb.parse()
    # print(next(pages))
    # print(next(pages))
    innodb.output()

if __name__ == "__main__":
    import sys
    main(sys.argv[1])