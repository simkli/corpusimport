import argparse
import csv

from sqlalchemy import create_engine, MetaData, Table
from tqdm import tqdm

coca_subgenres = [
    {'ID': 101, 'name': 'SPOK:ABC'},
    {'ID': 102, 'name': 'SPOK:NBC'},
    {'ID': 103, 'name': 'SPOK:CBS'},
    {'ID': 104, 'name': 'SPOK:CNN'},
    {'ID': 105, 'name': 'SPOK:FOX'},
    {'ID': 106, 'name': 'SPOK:MSNBC'},
    {'ID': 107, 'name': 'SPOK:PBS'},
    {'ID': 108, 'name': 'SPOK:NPR'},
    {'ID': 109, 'name': 'SPOK:Indep'},
    {'ID': 114, 'name': 'FIC:Gen (Book)'},
    {'ID': 115, 'name': 'FIC:Gen (Jrnl)'},
    {'ID': 116, 'name': 'FIC:SciFi/Fant'},
    {'ID': 117, 'name': 'FIC:Juvenile'},
    {'ID': 118, 'name': 'FIC:Movies'},
    {'ID': 123, 'name': 'MAG:News/Opin'},
    {'ID': 124, 'name': 'MAG:Financial'},
    {'ID': 125, 'name': 'MAG:Sci/Tech'},
    {'ID': 126, 'name': 'MAG:Soc/Arts'},
    {'ID': 127, 'name': 'MAG:Religion'},
    {'ID': 128, 'name': 'MAG:Sports'},
    {'ID': 129, 'name': 'MAG:Entertain'},
    {'ID': 130, 'name': 'MAG:Home/Health'},
    {'ID': 131, 'name': 'MAG:Afric-Amer'},
    {'ID': 132, 'name': 'MAG:Children'},
    {'ID': 133, 'name': 'MAG:Women/Men'},
    {'ID': 135, 'name': 'NEWS:Misc'},
    {'ID': 136, 'name': 'NEWS:News_Intl'},
    {'ID': 137, 'name': 'NEWS:News_Natl'},
    {'ID': 138, 'name': 'NEWS:News_Local'},
    {'ID': 139, 'name': 'NEWS:Money'},
    {'ID': 140, 'name': 'NEWS:Life'},
    {'ID': 141, 'name': 'NEWS:Sports'},
    {'ID': 142, 'name': 'NEWS:Editorial'},
    {'ID': 144, 'name': 'ACAD:History'},
    {'ID': 145, 'name': 'ACAD:Education'},
    {'ID': 146, 'name': 'ACAD:Geog/SocSci'},
    {'ID': 147, 'name': 'ACAD:Law/PolSci'},
    {'ID': 148, 'name': 'ACAD:Humanities'},
    {'ID': 149, 'name': 'ACAD:Phil/Rel'},
    {'ID': 150, 'name': 'ACAD:Sci/Tech'},
    {'ID': 151, 'name': 'ACAD:Medicine'},
    {'ID': 152, 'name': 'ACAD:Misc'}
]

def get_sources_coca(line):
    return {
        'id': line[0],
        'year': line[1],
        'genre': line[2],
        'subgenreID': line[3],
        'sourceTitle': line[4],
        'textTitle': line[5]
    }


def get_lexicon(line):
    return {
        'id': line[0],
        'word': line[1],
        'lemma': line[2],
        'PoS': line[3]
    }

def get_text(line):
    return {
        'ID': line[1],
        'textID': line[0],
        'wordID': line[2]
    }

def get_entries(file, extractor):
    with open(file, errors='ignore') as fh:
        reader = csv.reader(fh, delimiter='\t')
        for line in reader:
            if len(line) <= 3:
                continue
            yield extractor(line)


def create_cmd(db, args):
    with db.connect() as connection:
        connection.execute("""
            create table lexicon
            (
                ID    int auto_increment
                    primary key,
                word  text null,
                lemma text null,
                PoS   text null
            );
        """)
        connection.execute("""
            create table source
            (
                ID          int auto_increment
                    primary key,
                genre       varchar(255) null,
                subgenreID  int          null,
                year        int(4)       null,
                sourceTitle text         null,
                textTitle   text         null
            );
        """)
        connection.execute("""
            create index text_genre_id
                on source (subgenreID);
        """)
        connection.execute("""
            create table subgenre
            (
                ID   int          not null
                    primary key,
                name varchar(255) null
            );
        """)
        connection.execute("""
            create table text
            (
                ID     int auto_increment
                    primary key,
                textID int null,
                wordID int null
            );
        """)
        connection.execute("""
            create index corpus_lexicon_wordID
                on text (wordID); 
        """)
        connection.execute("""
            create index corpus_source_id
                on text (textID);
                """)
    metadata = MetaData(db)
    subgenre_table = Table('subgenre', metadata, autoload=True)
    db.execute(subgenre_table.insert(), coca_subgenres)

def import_cmd(db, args):
    metadata = MetaData(db)
    source_table = Table('source', metadata, autoload=True)
    lexicon_table = Table('lexicon', metadata, autoload=True)
    text_table = Table('text', metadata, autoload=True)

    with db.connect() as connection:
        transaction = connection.begin()
        try:
            if args.table == 'lexicon':
                for row in tqdm(get_entries(args.file, get_lexicon)):
                    connection.execute(lexicon_table.insert(), row)
            elif args.table == 'source':
                for row in tqdm(get_entries(args.file, get_sources_coca)):
                    connection.execute(source_table.insert(), row)
            elif args.table == 'text':
                for row in tqdm(get_entries(args.file, text_table)):
                    connection.execute(source_table.insert(), row)
            transaction.commit()
        except KeyboardInterrupt as e:
            print(e)
            print("rollback...")
            transaction.rollback()

def get_db(args):
    dbstr = 'mysql+pymysql://' + args.user
    if args.password:
        dbstr += ':' + args.password
    dbstr += '@' + args.host + ':' + str(args.port) + '/' + args.database
    return create_engine(dbstr)





if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host",
        default='localhost',
        help="database host"
    )
    parser.add_argument(
        "-u",
        "--user",
        default='root',
        help="database username"
    )
    parser.add_argument(
        "-p",
        "--password",
        help="database password"
    )
    parser.add_argument(
        "--port",
        default=3306,
        type=int,
        help="database port"
    )
    parser.add_argument(
        "-d",
        "--database",
        default='elia',
        help="database name"
    )

    subparsers = parser.add_subparsers(help='sub-command help')

    create_parser = subparsers.add_parser('create', help='creates the database tables and populates the subgenre table')
    create_parser.set_defaults(func=create_cmd)

    import_parser = subparsers.add_parser('import', help='imports data into a table')
    import_parser.set_defaults(func=import_cmd)
    import_parser.add_argument(
        "table",
        choices=['lexicon', 'source', 'text']
    )
    import_parser.add_argument(
        "file"
    )

    args = parser.parse_args()
    db = get_db(args)
    args.func(db, args)
