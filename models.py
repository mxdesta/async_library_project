"""
Создать набор моделей SQLAlchemy ORM для системы управления библиотекой с 
использованием PostgreSQL. Но уже асинхронно

"""

from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, validates, relationship
from sqlalchemy import Integer, CheckConstraint, and_, DateTime, String, ForeignKey, Text, func, Date, Boolean, Index, text, case, event
from datetime import datetime, date, timedelta
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.associationproxy import association_proxy



class Base(DeclarativeBase): #чтоб наследование было
    pass

class Author(Base):
    
    __tablename__ = 'author'
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    bio: Mapped[str] = mapped_column(Text, nullable=True)
    birth_date: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    
    
    books: Mapped[list['Book']] = relationship(back_populates='author') # создать в book поле author
    

class Book(Base):
    
    __tablename__ = 'book'
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    isbn: Mapped[str] = mapped_column(String(13), unique=True, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=True)
    published_year: Mapped[int] = mapped_column(Integer, nullable=False)
    genre: Mapped[str] = mapped_column(String(50), nullable=True)
    author_id: Mapped[int] = mapped_column(Integer, ForeignKey('author.id'), nullable=False, unique=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    
    author: Mapped['Author'] = relationship(back_populates='books') #у автора есть книги
    loans: Mapped[list['Loan']] = relationship(back_populates='book')
    
    
    @validates('isbn')
    def check_isbn(self, key, value):
        if len(value) != 13:
            raise ValueError("Isbn не содержит 13 символов")
        for elem in value:
            if not elem.isdigit():
                raise ValueError("Ошибка в ISBN, встретили не цифру")
        return value
            
    """__table_args__ = ( #Либо так
        CheckConstraint(
            'published_year >= 1000 AND published_year <= EXTRACT(YEAR FROM CURRENT_DATE)'
        ))
"""
    __table_args__ = ( #либо так, получше
        CheckConstraint(
            and_(
                published_year >= 1000,
                published_year <= func.extract('year', func.current_date())
            )
        ),
    )
    
    
    
class Reader(Base):
    
    __tablename__ = 'reader'
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    full_name: Mapped[str] = mapped_column(String(150), nullable=False)
    phone: Mapped[str] = mapped_column(String(20), nullable=True)
    address: Mapped[str] = mapped_column(Text, nullable=True)
    registered_at: Mapped[date] = mapped_column(Date, server_default=func.current_date())
    is_active: Mapped[bool] = mapped_column(Boolean, server_default='True')
    
    loans: Mapped[list['Loan']] = relationship(back_populates='reader')
    
    books_taken = association_proxy('loans', 'book') # для связи loans.book, отсюда можем их смоетрть
    
    @validates('email')
    def check_email(self, key, value:str):
        emails = ['@mail.ru', '@yandex.ru', '@gmail.com']
        for email in emails:
            if email in value:
                return value
        raise ValueError('Это не емейл!')
    
    @validates('phone')
    def check_phone(self, key, value:str):
        if value:
            if (value.startswith('+7') or value.startswith('8')) and (len(value) == 12 or len(value) == 11):
                return value
            raise ValueError("неверный формат номера")
        return None
    
    
class Loan(Base):
    
    __tablename__ = 'loan'
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    book_id: Mapped[int] = mapped_column(Integer, ForeignKey('book.id'), nullable=False)
    reader_id: Mapped[int] = mapped_column(Integer, ForeignKey('reader.id'), nullable=False)
    loan_date: Mapped[date] = mapped_column(Date, server_default=func.current_date())
    due_date: Mapped[date] = mapped_column(Date, nullable=False)
    return_date: Mapped[date] = mapped_column(Date, nullable=True, server_default=None)
    
    reader: Mapped['Reader'] = relationship(back_populates='loans')
    book: Mapped['Book'] = relationship(back_populates='loans')
    
    @hybrid_property
    def status(self) -> str:
        try:
            if self.return_date is None and self.due_date >= date.today():
                status = 'active'
            elif self.return_date is None and self.due_date < date.today():
                status = 'overdue'
            else:
                status = 'returned'
                
            return status
            
        except Exception as e:
            print('Ошибка!', e)
            raise
        
    
    @status.expression
    def status(cls):
        return case(
            (cls.return_date.isnot(None), 'returned'),
            (cls.due_date < func.current_date(), 'overdue'),
            else_= 'active'          
        )
        
    __table_args__ = (Index(
        'id_return_date_idx',
        'book_id',
        'return_date',
        unique=True,
        postgresql_where=text('return_date IS NULL')
    ),
                      )

    
@event.listens_for(Loan, 'before_insert')
def set_due_date(mapper, connection, target):
    if target.due_date is None:
        target.due_date = target.loan_date + timedelta(days=14)
        
@event.listens_for(Loan, 'after_insert')
def print_message(mapper, connection, target):
    if target.reader and target.book:
        print(f"Книга {target.book.title} выдана читателю {target.reader.full_name}")
    else:
        print('Данные не подгружены')
    

class LoanStatus:
    ACTIVE = 'active'
    OVERDUE = 'overdue'
    RETURNED = 'returned'