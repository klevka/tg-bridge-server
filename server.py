import asyncio
import os
import urllib.parse
import websockets

async def tcp_to_ws(remote_reader, ws):
    """Пересылает данные из Telegram TCP обратно в WebSocket на ПК."""
    try:
        while True:
            data = await remote_reader.read(16384)
            if not data:
                break
            await ws.send(data)
    except Exception:
        pass

async def ws_handler(ws):
    """Обрабатывает входящее WebSocket-подключение от вашего ПК."""
    # Извлекаем путь и параметры из запроса веб-сокета
    parsed_path = urllib.parse.urlparse(ws.path)
    query_params = urllib.parse.parse_qs(parsed_path.query)
    
    # Достаем хост и порт из параметров 'h' (host) и 'p' (port). 
    # Если их нет, используем дефолтный IP Telegram
    target_host = query_params.get("h", ["149.154.167.50"])[0]
    target_port = int(query_params.get("p", ["443"])[0])
    
    print(f"Получен запрос туннеля к Telegram-серверу -> {target_host}:{target_port}")
    
    try:
        # Открываем чистое TCP соединение из облака до серверов Telegram
        remote_reader, remote_writer = await asyncio.open_connection(target_host, target_port)
    except Exception as e:
        print(f"Не удалось связаться с Telegram {target_host}: {e}")
        await ws.close(1011, "Не удалось связаться с Telegram")
        return

    # Запускаем задачу чтения данных из Telegram в фоновом режиме
    bg_task = asyncio.create_task(tcp_to_ws(remote_reader, ws))

    try:
        # Читаем входящий поток байт от вашего ПК и транслируем его напрямую в Telegram
        async for message in ws:
            remote_writer.write(message)
            await remote_writer.drain()
    except Exception:
        pass
    finally:
        # Закрываем все сессии при разрыве соединения
        bg_task.cancel()
        remote_writer.close()
        try:
            await remote_writer.wait_closed()
        except Exception:
            pass

async def main():
    # Получаем динамический порт, который выделяет платформа Render
    port = int(os.environ.get("PORT", 8080))
    
    # Создаем асинхронное событие, чтобы удерживать сервер запущенным бесконечно
    stop_event = asyncio.Event()
    
    # Слушаем входящие соединения на всех интерфейсах
    async with websockets.serve(ws_handler, "0.0.0.0", port):
        print(f"Серверный WebSocket-мост успешно запущен на порту {port}")
        await stop_event.wait()

if __name__ == "__main__":
    asyncio.run(main())
