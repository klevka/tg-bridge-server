import asyncio
import os
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
    # Получаем целевые параметры Telegram из заголовков
    headers = dict(ws.request_headers)
    target_host = headers.get("X-Target-Host", "149.154.167.50")
    target_port = int(headers.get("X-Target-Port", "443"))

    try:
        # Открываем реальный, ничем не ограниченный TCP сокет до Telegram из дата-центра
        remote_reader, remote_writer = await asyncio.open_connection(target_host, target_port)
    except Exception:
        await ws.close(1011, "Не удалось связаться с Telegram")
        return

    # Запускаем чтение из Telegram в фоне
    bg_task = asyncio.create_task(tcp_to_ws(remote_reader, ws))

    try:
        # Принимаем данные от вашего ПК и шлем их в Telegram TCP
        async for message in ws:
            remote_writer.write(message)
            await remote_writer.drain()
    except Exception:
        pass
    finally:
        bg_task.cancel()
        remote_writer.close()
        try:
            await remote_writer.wait_closed()
        except Exception:
            pass


async def main():
    # Render выдает порт динамически в переменную окружения PORT
    port = int(os.environ.get("PORT", 8080))
    
    # Создаем асинхронное событие, чтобы удерживать сервер запущенным бесконечно
    stop_event = asyncio.Event()
    
    # Слушаем на всех интерфейсах 0.0.0.0
    async with websockets.serve(ws_handler, "0.0.0.0", port):
        print(f"Серверный WebSocket-мост успешно запущен на порту {port}")
        # Ждем, пока событие не будет вызвано (то есть бесконечно)
        await stop_event.wait()

if __name__ == "__main__":
    asyncio.run(main())

