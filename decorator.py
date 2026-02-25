from functools import wraps
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

def exception_decorator(func):
    """
    Декоратор для обработки исключений в CRUD-функциях.
    
    - Проверяет, что сессия передана и активна
    - Логирует начало выполнения
    - Разделяет ошибки SQLAlchemy и общие исключения
    - Пробрасывает исключения дальше
    """
    
    @wraps(func)
    async def wrapper(db: AsyncSession, *args, **kwargs):
        if not db.is_active:
            raise(RuntimeError(f"Сессия неактивна в {func.__name__}"))
        try:
            print(f'Выполняется функция {func.__name__}')
            return await func(db, *args, **kwargs)
        
    
        except OperationalError as e:
            print(f"Ошибка операции/соединения в {func.__name__}: {e}")
            raise
        
        except IntegrityError as e:
            print(f"Ошибка целостности в {func.__name__} - {e}")
            raise
        
        except SQLAlchemyError as e:
            print(f'Во время выполнения {func.__name__} произошла ошибка уровня sqlalchemy {e}')
            raise
        
        except Exception as e:
            print(f'Во время выполнения {func.__name__} произошла ошибка {e}')
            raise
    return wrapper
            