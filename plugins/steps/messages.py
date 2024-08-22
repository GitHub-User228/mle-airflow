from pathlib import Path

from airflow.providers.telegram.hooks.telegram import TelegramHook

from utils import read_yaml


tg_callback_config = read_yaml(Path("/opt/airflow/config/tg_callback.yaml"))


def send_telegram_success_message(
    context,
):
    hook = TelegramHook(
        # telegram_conn_id="tg_callback",
        telegram_conn_id="telegram_default",
        token=tg_callback_config["token_id"],
        chat_id=tg_callback_config["chat_id"],
    )
    message = (
        f"DAG {context['dag']} with run_id {context['run_id']} has been "
        f"successfully executed."
    )
    hook.send_message(
        {"chat_id": tg_callback_config["chat_id"], "text": message}
    )


def send_telegram_failure_message(context):
    hook = TelegramHook(
        # telegram_conn_id="tg_callback",
        telegram_conn_id="telegram_default",
        token=tg_callback_config["token_id"],
        chat_id=tg_callback_config["chat_id"],
    )
    message = (
        f"DAG {context['dag']} execution with run_id {context['run_id']} "
        f"has failed: {context['task_instance_key_str']}"
    )
    hook.send_message(
        {"chat_id": tg_callback_config["chat_id"], "text": message}
    )
