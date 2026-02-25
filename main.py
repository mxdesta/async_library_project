import asyncio
from database import get_db, init_db, close_db_engine
import crud
from datetime import date, timedelta

async def test_authors():
    """
    Тестируем операции с авторами
    """
    
    async with get_db() as db:

        author = await crud.create_author(
            db=db, 
            name="Лев Толстой",
            bio="Великий русский писатель",
            birth_date=date(1828, 9, 9)
        )
        print(f"Создан автор: {author.name}, id={author.id}")
        
        # Получение по ID
        fetched = await crud.get_author_by_id(db, author.id)
        print(f"Получен автор: {fetched.name}")
        
        # Обновление
        updated = await crud.update_author(db, author.id, bio="Обновлённая биография")
        print(f"Обновлён автор: {updated.bio}")
        
        # Получение всех
        all_authors = await crud.get_all_authors(db)  # переименуй функцию!
        print(f"Всего авторов: {len(all_authors)}")
        
        # Удаление
        deleted = await crud.delete_author(db, author.id)
        print(f"Автор удалён: {deleted}")

async def test_books():
    """
    Тестируем операции с книгами
    """

    async with get_db() as db:
        # Сначала создадим автора для книг
        author = await crud.create_author(db, name="Тестовый автор")
        
        # Создание книги
        book = await crud.create_book(
            db=db,
            title="Война и мир",
            isbn="9783161484100",
            published_year=1869,
            author_id=author.id,
            description="Роман-эпопея",
            genre="Роман"
        )
        print(f"Создана книга: {book.title}, id={book.id}")
        
        # Получение книги с автором и выдачами
        fetched = await crud.get_book_by_id(db, book.id)
        print(f"Автор книги: {fetched.author.name}")
        
        # Книги по автору
        author_books = await crud.get_books_by_author(db, author.id)
        print(f"Книг у автора: {len(author_books)}")
        
        # Очистка
        await crud.delete_book(db, book.id)
        await crud.delete_author(db, author.id)

async def test_readers():
    """
    Тестируем операции с читателями
    """


    async with get_db() as db:
        # Создание
        reader = await crud.create_reader(
            db=db,
            email="test@mail.ru",
            full_name="Иван Иванов",
            phone="+71234567890",
            address="Москва"
        )
        print(f"Создан читатель: {reader.full_name}, id={reader.id}")
        
        # Получение по email
        by_email = await crud.get_reader_by_email(db, "test@mail.ru")
        print(f"Найден по email: {by_email.full_name}")
        
        # Деактивация
        deactivated = await crud.deactivate_reader(db, reader.id)
        print(f"Деактивирован: {deactivated.is_active}")
        
        # Активация
        activated = await crud.activate_reader(db, reader.id)
        print(f"Активирован: {activated.is_active}")
        
        # Очистка
        await crud.delete_reader(db, reader.id)

async def test_loans():
    """
    Тестируем операции с выдачами
    """


    async with get_db() as db:
        # Создаём тестовые данные
        author = await crud.create_author(db, name="Автор для выдач")
        book = await crud.create_book(
            db=db,
            title="Книга для выдач",
            isbn="9783161484117",
            published_year=2020,
            author_id=author.id
        )
        reader = await crud.create_reader(
            db=db,
            email="reader@mail.ru",
            full_name="Читатель Петров"
        )
        
        # Создание выдачи
        loan = await crud.create_loan(
            db=db,
            book_id=book.id,
            reader_id=reader.id,
            due_date=date.today() + timedelta(days=14)
        )
        print(f"Создана выдача id={loan.id}, статус: {loan.status}")
        
        # Получение по ID
        fetched = await crud.get_loan_by_id(db, loan.id)
        print(f"Получена выдача: книга {fetched.book.title}, читатель {fetched.reader.full_name}")
        
        # Активные выдачи
        active = await crud.get_active_loans(db)
        print(f"Активных выдач: {len(active)}")
        
        # Просроченные (создадим просрочку)
        overdue_loan = await crud.create_loan(
            db,
            book_id=book.id,  # тут будет конфликт! книга уже выдана
            reader_id=reader.id,
            due_date=date.today() - timedelta(days=5)  # просрочка
        )
        # Ждём конфликта от unique index
        
        # Возврат книги
        returned = await crud.return_book(db, loan.id)
        print(f"Книга возвращена: {returned.return_date}")
        
        # Должники
        debtors = await crud.get_debtors(db)
        print(f"Должников: {len(debtors)}")
        
        # Очистка
        await crud.delete_loan(db, loan.id)
        await crud.delete_loan(db, overdue_loan.id) 
        await crud.delete_reader(db, reader.id)   
        await crud.delete_book(db, book.id)
        await crud.delete_author(db, author.id)

async def main():
    
    print('Инициализируем БД')
    await init_db()
    
    try:
        await test_authors()
        await test_books()
        await test_readers()
        await test_loans()
        
    except Exception as e:
        print(f"Возникла ошибка {e}")
        raise
    
    finally:
        await close_db_engine()
        

if __name__ == "__main__":
    asyncio.run(main())