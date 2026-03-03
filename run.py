import sys
import os

def task_metricas_soporte(args: list):
    from src.support_metrics import main as m
    sys.argv = ["metricas_soporte"] + args
    m.main()

switcher = {
    "metricas_soporte": task_metricas_soporte
}


def execute_task():
    """Valida argumentos y despacha la ejecución a la función correspondiente."""

    if len(sys.argv) < 2:
        print("❌ [ERROR] Falta el nombre de la tarea.")
        print(f"   Uso: python {os.path.basename(__file__)} <nombre_tarea> [args...]")
        print(f"   Tareas disponibles: {list(switcher.keys())}")
        sys.exit(1)

    task_name = sys.argv[1]
    extra_args = sys.argv[2:]
    func = switcher.get(task_name)

    if func:
        print(f"🚀 [RUN] Iniciando tarea: '{task_name}'")
        if extra_args:
            print(f"   ⚙️ Argumentos: {extra_args}")

        try:
            func(extra_args)
            print(f"✅ [RUN] Tarea '{task_name}' finalizada correctamente.")
        except Exception as e:
            print(f"❌ [RUN] Error crítico en '{task_name}': {e}")
            raise  # Elevar excepción para notificar fallo a Cloud Run
    else:
        print(f"❌ [ERROR] Tarea desconocida: '{task_name}'")
        sys.exit(1)


if __name__ == "__main__":
    execute_task()
