import models
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from decorator import exception_decorator
from typing import Sequence, Optional
from datetime import date
from sqlalchemy.orm import selectinload




"""------- AUTHOR ------"""
@exception_decorator
async def get_author_by_id(db: AsyncSession, author_id: int) -> Optional[models.Author]:
    """
    Получить автора по ID с предзагрузкой книг

    Args:
        db (AsyncSession): Асинхронная сессия SQLalchemy
        author_id (int): ID автора

    Returns:
        Optional[models.Author] (объект Автора или None, если не нашли)
    """
    stmt = (
        select(models.Author)
        .where(models.Author.id == author_id)
        .options(
            selectinload(models.Author.books)
        )
        )
    result = await db.execute(statement=stmt)
    return result.scalar_one_or_none()


@exception_decorator
async def get_all_authors(db: AsyncSession, skip: int=0, limit: int=100) -> Sequence[models.Author]:
    """
    Получить всех авторов подряд, можно указать сколько пропустить и сколько надо получить

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        skip (int, optional): Сколько пропускаем от начала (OFFSET). Defaults to 0.
        limit (int, optional): Сколько нужно вывести. Defaults to 100.

    Returns:
        Sequence[models.Author]: Набор объектов Author
    """
    stmt = (
        select(models.Author)
        .offset(skip)
        .limit(limit)
        .options(
            selectinload(models.Author.books)
        )
        )
    result = await db.execute(statement=stmt)
    return result.scalars().all()


    
@exception_decorator
async def create_author(
    db: AsyncSession, 
    name: str, 
    bio: Optional[str] = None, 
    birth_date: Optional[date] = None
    ) -> models.Author:
    """
    Создаем автора

    Args:
        db (AsyncSession): Активная сеессия SQLalchemy
        name (str): Имя автора
        bio (Optional[str], optional): Биография автора. Defaults to None.
        birth_date (Optional[date], optional): Дата рождения автора. Defaults to None.

    Returns:
        models.Author: Возвращаем созданный объект автора.
        
    """
    
    author = models.Author(
        name = name,
        bio = bio,
        birth_date = birth_date
    )
    db.add(author)
    await db.flush() #отправляем в базу
    await db.refresh(author)
    return author



@exception_decorator
async def update_author(db: AsyncSession, author_id: int,  **kwargs) -> Optional[models.Author]:
    """
    Изменяем указанные поля у Author с определенным ID

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        author_id (int): ID автора

    Returns:
        Optional[models.Author]: Измененный объект Author, либо None, если такого нет
    """
    stmt = (
        update(models.Author)
        .where(models.Author.id == author_id)
        .values(kwargs)
        .returning(models.Author)
        
            
            )
    author = await db.execute(statement=stmt)
    return author.scalar_one_or_none()


@exception_decorator
async def delete_author(db: AsyncSession, author_id: int) -> bool:
    """
    Удалить автора с указанным ID

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        book_id (int): ID книги

    Returns:
        bool: True, при успешном удалении, False при неуспешном
    """
    
    stmt = (
        delete(models.Author)
        .where(models.Author.id == author_id) #удаляем по id, справа models.Author.id всегда
        
    )
    
    result = await db.execute(stmt)
    return result.rowcount > 0



"""-----BOOK-----"""

@exception_decorator
async def get_book_by_id(db: AsyncSession, book_id: int) -> Optional[models.Book]:
    """
    Получить книгу по заданному ID

    Args:
        db (AsyncSession): Активная сессия SQLaclhemy
        book_id (int): ID книги, которую хотим получить

    Returns:
        Optional[models.Book]: Объект Book, либо None
    """
    stmt = (
        select(models.Book)
        .where(models.Book.id == book_id)
        .options(
            selectinload(models.Book.author),
            selectinload(models.Book.loans)
        )
    )
    book = await db.execute(statement=stmt)
    return book.scalar_one_or_none()


@exception_decorator
async def get_all_books(db: AsyncSession, skip: int =0, limit: int = 100) -> Sequence[models.Book]:
    """
    Получить все книги с возможностью пропустить первые, указать всего сколько необходимо

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        skip (int, optional): Сколько книг первых пропустить. Defaults to 0.
        limit (int, optional): Сколько нужно вывести. Defaults to 100.

    Returns:
        Sequence[models.Book]: Последовательность объектов Book
    """
    stmt = (
        select(models.Book)
        .offset(skip)
        .limit(limit)
        .options(
            selectinload(models.Book.author),
            selectinload(models.Book.loans)
        )
    )
    result = await db.execute(statement=stmt)
    return result.scalars().all()
    
    
@exception_decorator
async def get_books_by_author(db: AsyncSession, author_id: int) -> Sequence[models.Book]:
    """
    Получить все книги определенного автора по его ID

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        author_id (int): ID автора, чьи книги мы хотим получить

    Returns:
        Sequence[models.Book]: Последовательность объектов Book
    """
    stmt = (
        select(models.Book)
        .where(models.Book.author_id == author_id)
        .options(
            selectinload(models.Book.author),
            selectinload(models.Book.loans)
        )
    )
    result = await db.execute(stmt)
    return result.scalars().all()


@exception_decorator
async def create_book(
    db: AsyncSession,
    title:str,
    isbn: str,
    published_year: int,
    author_id: int,
    description: Optional[str] = None,
    genre: Optional[str] = None
    ) -> Optional[models.Book]:
    """
    Создаем книгу, указав **kwargs

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        title (str): Название книги
        isbn (str): 13 цифр, уникальные у каждой книги
        published_year (int): Год публикации
        author_id (int): ID автора
        description (Optional[str], optional): Описание книги. Defaults to None.
        genre (Optional[str], optional): Жанр книги. Defaults to None.

    Returns:
        Optional[models.Book]: Вернется либо созданный объект класса Book, либо None
    """
    
    book = models.Book(title= title,
                       isbn = isbn,
                       published_year = published_year,
                       author_id = author_id,
                       description = description,
                       genre = genre)
    db.add(book)
    await db.flush()
    await db.refresh(book)
    return book

@exception_decorator
async def update_book(db: AsyncSession, book_id: int, **kwargs) -> Optional[models.Book]:
    """
    Изменить параметры у книги с определенным ID

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        book_id (int): ID книги, чьи параметры хотим изменить

    Returns:
        Optional[models.Book]: Измененный объект Book, либо None
    """
    stmt = (update(models.Book)
            .where(models.Book.id == book_id)
            .values(kwargs)
            .returning(models.Book)
            )
    book = await db.execute(statement=stmt)
    return book.scalar_one_or_none()

@exception_decorator
async def delete_book(db: AsyncSession, book_id: int) -> bool:
    """
    Удалить книгу с определенным ID

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        book_id (int): ID книги, чьи параметры хотим удалить

    Returns:
        bool: True, если успешно удалили или False
    """
    stmt = (
        delete(models.Book)
        .where(models.Book.id == book_id)
            )
    
    result = await db.execute(stmt)
    return result.rowcount > 0



"""-----READER-----"""


@exception_decorator
async def get_reader_by_id(db: AsyncSession, reader_id: int) -> Optional[models.Reader]:
    """
    Получить читателя по его ID

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        reader_id (int): ID читателя

    Returns:
        Optional[models.Reader]: Объект Reader или None
    """
    stmt = (
        select(models.Reader)
        .where(models.Reader.id == reader_id)
        .options(
            selectinload(models.Reader.loans)
        )
    )
    result = await db.execute(statement=stmt)
    return result.scalar_one_or_none()

@exception_decorator
async def get_all_readers(db: AsyncSession, skip: int = 0, limit: int = 100) -> Sequence[models.Reader]:
    """
    Получить всех читателей, можно пропустить
    skip в начале и задать ограничение на количество получаемых

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        skip (int, optional): Сколько пропустить от начала. Defaults to 0.
        limit (int, optional): Сколько требуется вывести. Defaults to 100.

    Returns:
        Sequence[models.Reader]: Набор объектов Reader
    """
    stmt = (
        select(models.Reader)
        .offset(skip)
        .limit(limit)
        .options(
            selectinload(models.Reader.loans)
        )
    )
    
    result = await db.execute(statement=stmt)
    return result.scalars().all()
    
    
@exception_decorator
async def get_reader_by_email(db: AsyncSession, email:str) -> Optional[models.Reader]:
    """
    Получить читателя по его email

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        email (str): email читателя

    Returns:
        Optional[models.Reader]: Объект Reader, либо None, если не нашли
    """
    stmt = (
        select(models.Reader)
        .where(models.Reader.email == email)
        .options(
            selectinload(models.Reader.loans)
        )
    )
    result = await db.execute(statement=stmt)
    return result.scalar_one_or_none()

@exception_decorator
async def create_reader(
    db: AsyncSession,
    email: str,
    full_name: str,
    phone: Optional[str] = None,
    address: Optional[str] = None
    ) -> Optional[models.Reader]:
    """
    Создать нового читателя

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        email (str): Email читателя.
        full_name (str): Полное имя читателя.
        phone (Optional[str], optional): Телефон читателя. Defaults to None.
        address (Optional[str], optional): Адресс читателя. Defaults to None.

    Returns:
        Optional[models.Reader]: Успешно созданный объект Reader, либо None
    """
    reader = models.Reader(
        email = email,
        full_name = full_name,
        phone = phone, 
        address = address,
    )
    db.add(reader)
    await db.flush()
    await db.refresh(reader)
    return reader


@exception_decorator
async def update_reader(db: AsyncSession, reader_id: int, **kwargs) -> Optional[models.Reader]:
    """
    Обновить информацию читателя, находим его по его ID

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        reader_id (int): ID читателя

    Returns:
        Optional[models.Reader]: Измененный объект Reader, либо None 
    """
    
    stmt = (
        update(models.Reader)
        .where(models.Reader.id == reader_id)
        .values(kwargs)
        .returning(models.Reader)
    )
    result = await db.execute(statement=stmt)
    return result.scalar_one_or_none()

@exception_decorator
async def delete_reader(db: AsyncSession, reader_id: int) -> bool:
    """
    Удалить читателя по его ID

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        reader_id (int): ID читателя

    Returns:
        bool: True в случае успешного удаления, False в противном случае
    """
    stmt = (
        delete(models.Reader)
        .where(models.Reader.id == reader_id)
    )
    result = await db.execute(stmt)
    return result.rowcount > 0


@exception_decorator
async def deactivate_reader(db: AsyncSession, reader_id: int) -> Optional[models.Reader]:
    """
    Установить читателя как неактивный (заморозить его)

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        reader_id (int): ID читателя

    Returns:
        Optional[models.Reader]: Объект Reader если успешно, None в противном случае
    """
    stmt = (
        update(models.Reader)
        .where(models.Reader.id == reader_id)
        .values(is_active = False)
        .returning(models.Reader)
    )
    result = await db.execute(statement=stmt)
    return result.scalar_one_or_none()



@exception_decorator
async def activate_reader(db: AsyncSession, reader_id: int) -> Optional[models.Reader]:
    """
    Установить читателя как активный (разморозить его)

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        reader_id (int): ID читателя

    Returns:
        Optional[models.Reader]: Объект Reader если успешно, None в противном случае
    """
    stmt = (
        update(models.Reader)
        .where(models.Reader.id == reader_id)
        .values(is_active = True)
        .returning(models.Reader)
    )
    result = await db.execute(statement=stmt)
    
    return result.scalar_one_or_none()

"""-----LOAN-----"""
"""Теперь будем делать через ORM, чтоб в нем потренироваться тоже"""

def _base_loan_query():
    """
    Предзагружаем book и reader для записи (чтоб не было лишних запросов)

    Returns:
        _type_: Select запрос с жадной загрузкой для коллекций
    """
    
    return select(models.Loan).options(
        selectinload(models.Loan.book),
        selectinload(models.Loan.reader)
    )

@exception_decorator
async def get_loan_by_id(db: AsyncSession, loan_id: int) -> Optional[models.Loan]:
    #если предзагрузка, то selectinload
    #по сути мой код тоже был отличный
    #переходим на ORM, может быть ленивая загрузка, N+1 и тд
    """loan: Optional[models.Loan] = await db.get(models.Loan, loan_id)
    return loan""" #сделали бы так, если бы не было предзагрузки других полей
    
    """
    Получить выдачу по ID
    
    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        loan_id (int): ID выдачи


    Returns:
        Optional[models.Loan]: Объект Loan если успешно, None в противном случае
    """

    stmt = (
        _base_loan_query()
        .where(models.Loan.id == loan_id)
        
    )
    result = await db.execute(statement=stmt)
    return result.scalar_one_or_none()


@exception_decorator
async def get_all_loans(db: AsyncSession, skip: int = 0, limit: int = 100) -> Sequence[models.Loan]:
    """
    Получить все выдачи с возможностью их пропустить
    и указать их лимит.
    

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        skip (int, optional): Сколько выдач пропустить от начала. Defaults to 0.
        limit (int, optional): Сколько нужно объектов. Defaults to 100.

    Returns:
        Sequence[models.Loan]: Набор объектов Loan
    """
    stmt = (
        _base_loan_query()
        .offset(skip)
        .limit(limit)
    )
    result = await db.execute(statement=stmt)
    return result.scalars().all()


@exception_decorator
async def get_active_loans(db: AsyncSession) -> Sequence[models.Loan]:
    """
    Получить активные выдачи

    Args:
        db (AsyncSession): Активная сессия SQLalchemy

    Returns:
        Sequence[models.Loan]: Набор объектов Loan, у которых models.Loan.status = 'active'
    """
    
    stmt = (
        _base_loan_query()
        .where(models.Loan.status == models.LoanStatus.ACTIVE) # type: ignore
    )
    
    result = await db.execute(statement=stmt)
    return result.scalars().all()


@exception_decorator
async def get_overdue_loans(db: AsyncSession) -> Sequence[models.Loan]:
    """
    Получить просроченных выдачи

    Args:
        db (AsyncSession): Активная сессия SQLalchemy

    Returns:
        Sequence[models.Loan]: Набор объектов Loan, у которых models.Loan.status = 'overdue'
    """
    stmt = (
        _base_loan_query()
        .where(models.Loan.status == models.LoanStatus.OVERDUE) # type: ignore
    )
    result = await db.execute(statement=stmt)
    return result.scalars().all()


@exception_decorator
async def get_loans_by_reader(db: AsyncSession, reader_id: int) -> Sequence[models.Loan]:
    """
    Получить выдачи у определенного читателя

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        reader_id (int): ID читателя

    Returns:
        Sequence[models.Loan]: Набор объектов Loan для конкретного читателя
    """
    stmt = (
        _base_loan_query()
        .where(models.Loan.reader_id == reader_id)

    )
    result = await db.execute(statement=stmt)
    return result.scalars().all()


@exception_decorator
async def get_loans_by_book(db: AsyncSession, book_id:int) -> Sequence[models.Loan]:
    """
    Получить выдачи для опреденной книги

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        book_id (int): ID книги

    Returns:
        Sequence[models.Loan]: Набор объектов Loan для конкретной книги
    """
    stmt = (
        _base_loan_query()
        .where(models.Loan.book_id == book_id)
    )
    result = await db.execute(statement=stmt)
    return result.scalars().all()

@exception_decorator
async def create_loan(
    db: AsyncSession,
    book_id: int,
    reader_id: int,
    loan_date: Optional[date] = None,
    due_date: Optional[date] = None 
    ) -> models.Loan:
    
    """
    Создать новую выдачу

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        book_id (int): ID книги
        reader_id (int): ID читателя
        loan_date (Optional[date], optional): Дата выдачи(в бд current_date). Defaults to None.
        due_date (Optional[date], optional): На сколько будет выдача (в бд на 14 дней). Defaults to None.

    Returns:
        models.Loan: Успешно созданный объект Loan
    """
    
    loan = models.Loan(
        book_id = book_id,
        reader_id = reader_id,
        loan_date = loan_date,
        due_date = due_date
    )
    
    db.add(loan)
    await db.flush()
    await db.refresh(loan)
    return loan


@exception_decorator
async def return_book(db: AsyncSession, loan_id: int) -> Optional[models.Loan]:
    """
    Вернуть книгу в библиотеку

    Args:
        db (AsyncSession): Активная сессия SQLalchemy
        loan_id (int): ID выдачи

    Returns:
        Optional[models.Loan]: Объект Loan, если успешно вернули книгу или None
    """
    stmt = (
        update(models.Loan)
        .where(models.Loan.id == loan_id)
        .values(return_date = date.today())
        .returning(models.Loan)
    )
    res = await db.execute(statement=stmt)
    return res.scalar_one_or_none()


@exception_decorator
async def get_debtors(db: AsyncSession) -> Sequence[models.Reader]:
    """
    Получить набор должников

    Args:
        db (AsyncSession): Активная сессия SQLalchemy

    Returns:
        Sequence[models.Reader]: Набор объектов Reader, кто должен книгу (models.Loan.status == 'overdue')
    """
    
    stmt = (
        select(models.Reader)
        .where(models.Reader.id == models.Loan.reader_id, models.Loan.status == models.LoanStatus.OVERDUE)# type: ignore
        .distinct()
    )
    result = await db.execute(statement=stmt)
    return result.scalars().all()


@exception_decorator
async def delete_loan(db: AsyncSession, loan_id: int) -> bool:
    """
    Удалить выдачу по ID.
    
    Args:
        db (AsyncSession): Активная сессия SQLAlchemy
        loan_id (int): ID выдачи
    
    Returns:
        bool: True, если удаление успешно, False если запись не найдена
    """
    stmt = (
        delete(models.Loan)
        .where(models.Loan.id == loan_id)
        .returning(models.Loan.id)  # возвращаем ID, чтобы знать, что удалили
    )
    
    result = await db.execute(stmt)
    
    return result.scalar_one_or_none() is not None