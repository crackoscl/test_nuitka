import polars as pl
import argparse
import os


def procesar_datos(archivo):
    if not os.path.exists(archivo):
        print(f"❌ Error: El archivo '{archivo}' no existe.")
        return

    if os.path.getsize(archivo) == 0:
        print("❌ El archivo está totalmente vacío (0 bytes).")
        return

    print(f"🚀 Leyendo archivo: {archivo}...")

    try:
        datos = pl.read_csv(archivo, separator=";", encoding="cp1252")

        resumen = (
            datos.with_columns(
                pl.col("Fecha Despacho").str.to_date().alias("Fecha")
            )
            .filter(pl.col("Frigorifico").is_in(["GREENVIC       ", "FRUTANGO S.A.  "]))
            .group_by(
                "Frigorifico",
                pl.col("Fecha").dt.year().alias("Año"),
                "Especie   ",
                "Embalaje"
            )
            .agg(
                pl.col("Cajas").sum().alias("Total_cajas")
            )
            .sort("Año", "Frigorifico")
        )

        if resumen.height == 0:
            print(
                "ℹ️ El archivo tiene datos, pero ninguno coincide con los frigoríficos buscados.")
            return

        output_name = "resumen_procesado.csv"
        resumen.write_csv(output_name, separator=";")
        print(f"✅ ¡Éxito! Resumen guardado como '{output_name}'.")

    except Exception as e:
        print(f"💥 Ocurrió un error inesperado: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Herramienta para procesar exportaciones de fruta.",
        usage="python main.py [RUTA_DEL_ARCHIVO]",
        add_help=False
    )

    parser.add_argument(
        "archivo",
        help="Ruta al archivo CSV (ej: datos.csv)",
        nargs="?"
    )

    parser.add_argument(
        "-h", "--help",
        action="help",
        help="Muestra este mensaje de ayuda."
    )

    args = parser.parse_args()

    ruta = args.archivo
    if ruta is None:
        print("💡 No indicaste el archivo por argumento.")
        ruta = input("👉 Por favor, escribe su nombre: ").strip('"').strip("'")

    if ruta:
        procesar_datos(ruta)
    else:
        print("⚠️ No se proporcionó ninguna ruta. Saliendo...")
