import polars as pl


datos = pl.read_csv("EMBDEUNI.TXT",separator=";",encoding="cp1252")
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


print(resumen)
input("\nPrograma terminado. Presiona Enter para cerrar esta ventana.")