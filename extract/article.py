from sqlalchemy import Column, String
from base import Base


class Article(Base):
    __tablename__ = 'articles'

    id = Column(String, primary_key=True)
    body_csv = Column(String)
    host = Column(String)
    title_csv = Column(String)
    newspapper_uid = Column(String)
    n_token_body_csv = Column(String)
    n_token_title_csv = Column(String)
    url_csv = Column(String, unique=True)

    def __init__(self,
                 uid,
                 body_csv,
                 host,
                 newspapper_uid,
                 n_token_body_csv,
                 n_token_title_csv,
                 title_csv,
                 url_csv):
        self.id = uid
        self.body_csv = body_csv
        self.host = host
        self.newspapper_uid = newspapper_uid
        self.n_token_body_csv = n_token_body_csv
        self.n_token_title_csv = n_token_title_csv
        self.title_csv = title_csv
        self.url_csv = url_csv
