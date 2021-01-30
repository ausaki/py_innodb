
import os
import struct


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
    OVERVIEW_OUTPUT_MAX_PAGES = 200

    def __init__(self, ibd_file):
        self._file = ibd_file
    
    def parse(self, max_pages=1000):
        with open(self._file, 'rb') as fp:
            fsize = os.fstat(fp.fileno()).st_size
            npages = fsize // Page.PAGE_SIZE
            for i in range(min(max_pages, npages)):
                page = fp.read(Page.PAGE_SIZE)
                page = Page(page)
                yield page
        
    def overview_output(self):
        pages = list(self.parse())
        npages = 0
        g = {}
        for page in pages:
            npages += 1
            if npages <= self.OVERVIEW_OUTPUT_MAX_PAGES: 
                print(page)
            g[page.type_str] = g.get(page.type_str, 0) + 1
            
        print()
        print('Total number of page: {}'.format(npages))
        for k, v in g.items():
            print('{}: {}'.format(k, v))
    
    def page_verbose_output(self, pageno):
        page = self.pages[pageno]
        print(page.verbose_info())


class Record:
    pass

class RecordCompact(Record):
    def __init__(self) -> None:
        self.offset = None
        self.next_record = None
        self.header = None
        self.data = None

    @classmethod
    def from_page(cls, page, offset):
        _page = page._page
        record = cls()
        record.offset = offset
        record.header = _page[offset - 6:offset]
        record.next_record = struct.unpack('>h', record.header[-2:])[0]
        record.data = _page[offset:offset + 50]
        return record

    def __str__(self) -> str:
        s = 'record offset: {:04x}, header: {}, data(20 bytes): {}'.format(self.offset, self.header.hex(), self.data[:20].hex())
        return s


class PageMeta(type):
    def __new__(cls, name, bases, attrs) -> None:
        # for _, k in attrs['FIL_HDR_FMT']:
        #     attrs[k] = None
        # for _, k in attrs['PAGE_HDR_FMT']:
        #     attrs[k] = None
        return super().__new__(cls, name, bases, attrs)


class Page(metaclass=PageMeta):
    
    # page size
    PAGE_SIZE = 1 << 14

    FIL_HDR_FMT = (
        ('I', 'checksum'),
        ('I', 'offset'),
        ('I', 'prev_page'),
        ('I', 'next_page'),
        ('Q', 'lsn'),
        ('H', 'type'),
        ('Q', 'file_flush_lsn'),
        ('I', 'space_id'),
    )
    PAGE_HDR_FMT = (
        ('H', 'dir_slots'),
        ('H', 'heap_top'),
        ('H', 'nheap'),
        ('H', 'free_list'),
        ('H', 'garbage'),
        ('H', 'last_insert'),
        ('H', 'direction'),
        ('H', 'ndirection'),
        ('H', 'nrecords'),
        ('Q', 'max_trx_id'),
        ('H', 'level'),
        ('Q', 'index_id'),
        ('10s', 'btr_seg_leaf'),
        ('10s', 'btr_seg_top'),
    )

    # page types. for more details see https://github.com/mysql/mysql-server/blob/8.0/storage/innobase/include/fil0fil.h#L1201
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
    
    @classmethod
    def page_type_str(cls, page_type):
        return cls.PAGE_TYPES.get(page_type, 'UNKNOWN({:04x})'.format(page_type))

    def __init__(self, page):
        self._page = page
        self.parse()

    def parse(self):
        file_header_fmt = '>' + ''.join(f for f, _ in self.FIL_HDR_FMT)
        file_header_attrs = [attr for _, attr in self.FIL_HDR_FMT]
        page_header_fmt = '>' + ''.join(f for f, _ in self.PAGE_HDR_FMT)
        page_header_attrs = [attr for _, attr in self.PAGE_HDR_FMT]
        
        sz = struct.calcsize(file_header_fmt)
        vals = struct.unpack(file_header_fmt, self._page[:sz])
        for attr, val in zip(file_header_attrs, vals):
            if isinstance(attr, str) and len(attr) > 0:
                setattr(self, attr, val)
        
        sz2 = struct.calcsize(page_header_fmt)
        vals = struct.unpack(page_header_fmt, self._page[sz: sz + sz2])
        for attr, val in zip(page_header_attrs, vals):
            if isinstance(attr, str) and len(attr) > 0:
                setattr(self, attr, val)

        # page directory
        if self.type == self.PAGE_TYPE_BTREE_NODE:
            directory_size = self.dir_slots * 2
            directory = self._page[-(directory_size + 8): -8]
            dirfmt = '>' + 'H' * self.dir_slots
            self.directory = struct.unpack(dirfmt, directory)
            self.parse_records()
        # file trailer
        self.file_trailer = self._page[-8:]

        # free page
        self._page = None

    @property
    def type_str(self):
        return self.page_type_str(self.type)

    def verbose_info(self):
        s = ['== Page Info ==']
        for field in ['offset', 'type', 'level', 'dir_slots', 'heap_top', 'nheap', 
                    'free_list', 'direction', 'ndirection', 'nrecords', 'index_id']:
            v = getattr(self, field, None)
            if field == 'type':
                v = self.type_str
            else:
                v = '{:04x}'.format(v)
            s.append('{}: {}'.format(field, v))
        if self.type == self.PAGE_TYPE_BTREE_NODE:
            s.append('== Records ==')
            s.extend(str(rec) for rec in self.records)
        return '\n'.join(s)
    
    def simple_info(self):
        s = 'page offset: {:08x}, page type: {}'.format(self.offset, self.type_str)
        if self.type == self.PAGE_TYPE_BTREE_NODE:
            s += ', page level: {:04x}'.format(self.level)
        return s

    def parse_records(self):
        offset = self.directory[-1]
        records = []
        record = RecordCompact.from_page(self, offset)
        records.append(record)
        while record.next_record:
            offset = record.offset + record.next_record
            record = RecordCompact.from_page(self, offset)
            records.append(record)
        self.records = records

    def __str__(self) -> str:
        return self.simple_info()


def main():
    import sys
    import argparse
    parser = argparse.ArgumentParser('pyibd')
    parser.add_argument('ibd_file', help='table space file', action='store')
    parser.add_argument('-p', '--page', action='store', type=int, default='-1', help='show page info')
    args = parser.parse_args(sys.argv[1:])
    
    innodb = InnoDB(args.ibd_file)
    if args.page >= 0:
        innodb.page_output(args.page)
        return
    innodb.overview_output()

if __name__ == "__main__":
    main()

    