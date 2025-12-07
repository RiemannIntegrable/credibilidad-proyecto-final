# Celdas Restantes para Implementar

## Cell 14: Validación de Conservación de Exposición
```python
# Validar que la suma de exposiciones prorrateadas ≈ exposición original
print("=" * 80)
print("VALIDACIÓN: CONSERVACIÓN DE EXPOSICIÓN")
print("=" * 80)

# Agrupar por vigencia original y sumar exposiciones prorrateadas
validacion = expuestos_periodos.groupby('VIGENCIA_ORIGINAL_ID').agg({
    'EXPOSICION_PRORRATEADA': 'sum',
    'EXPOSICION_ORIGINAL': 'first'
}).reset_index()

validacion['DIFERENCIA'] = validacion['EXPOSICION_PRORRATEADA'] - validacion['EXPOSICION_ORIGINAL']
validacion['DIFERENCIA_PCT'] = (validacion['DIFERENCIA'] / validacion['EXPOSICION_ORIGINAL']) * 100

print(f"\nEstadísticas de conservación de exposición:")
print(f"  Diferencia promedio: ${validacion['DIFERENCIA'].mean():,.2f}")
print(f"  Diferencia máxima: ${validacion['DIFERENCIA'].abs().max():,.2f}")
print(f"  Diferencia % promedio: {validacion['DIFERENCIA_PCT'].mean():.6f}%")
print(f"  Diferencia % máxima: {validacion['DIFERENCIA_PCT'].abs().max():.6f}%")

# Verificar si la conservación es adecuada (< 1% de diferencia)
if validacion['DIFERENCIA_PCT'].abs().max() < 1.0:
    print(f"\n✅ VALIDACIÓN EXITOSA: La exposición se conserva correctamente (diferencia < 1%)")
else:
    print(f"\n⚠️  ADVERTENCIA: Hay diferencias significativas en la conservación de exposición")
```

## Cell 15: Crear ID Único de Período
```python
# Crear ID único para cada período: POLIZA_AMPARO_AÑO_PERIODO_YYYYMMDD
expuestos_periodos['ID_POLIZA_AMPARO_PERIODO'] = (
    expuestos_periodos['POLIZA'].astype(str) + '_' +
    expuestos_periodos['AMPARO'] + '_' +
    expuestos_periodos['ANIO_POLIZA'].astype(str) + '_' +
    expuestos_periodos['PERIODO'].astype(str) + '_' +
    expuestos_periodos['VIGENCIA_PERIODO_INICIO'].dt.strftime('%Y%m%d')
)

print(f"IDs únicos creados: {expuestos_periodos['ID_POLIZA_AMPARO_PERIODO'].nunique():,}")
print(f"Total de períodos: {len(expuestos_periodos):,}")
print(f"\nPrimeros 5 IDs:")
for id_ejemplo in expuestos_periodos['ID_POLIZA_AMPARO_PERIODO'].head():
    print(f"  - {id_ejemplo}")
```

## Cell 16: Normalización de Amparos (YA EXISTE - MANTENER)

## Cell 17: Preparar Fechas de Siniestros
```python
# Convertir fecha de siniestro a datetime
siniestros_consolidado['FECHA_SINIESTRO_DT'] = pd.to_datetime(
    siniestros_consolidado['FECHA_DE_SINIESTRO'],
    format='%d/%m/%Y',
    errors='coerce'
)

print(f"Fechas de siniestro convertidas:")
print(f"  Total siniestros: {len(siniestros_consolidado):,}")
print(f"  Con fecha válida: {siniestros_consolidado['FECHA_SINIESTRO_DT'].notna().sum():,}")
print(f"  Con fecha inválida: {siniestros_consolidado['FECHA_SINIESTRO_DT'].isna().sum():,}")
```

## Cell 18: JOIN Temporal - Asignar Siniestros a Períodos
```python
print("=" * 80)
print("JOIN TEMPORAL: ASIGNANDO SINIESTROS A PERÍODOS")
print("=" * 80)
print(f"\nProcesando {len(siniestros_consolidado):,} siniestros...")
print("Esto puede tomar varios minutos...\n")

siniestros_no_asignados = []
periodos_con_siniestro = []

# Procesar cada siniestro
for idx, sin_row in siniestros_consolidado.iterrows():
    if idx % 1000 == 0 and idx > 0:
        print(f"  Procesados: {idx:,} / {len(siniestros_consolidado):,}")

    poliza = sin_row['POLIZA']
    amparo = sin_row['AMPARO']
    fecha_siniestro = sin_row['FECHA_SINIESTRO_DT']

    # Buscar períodos que contengan la fecha de siniestro
    periodos_match = expuestos_periodos[
        (expuestos_periodos['POLIZA'] == poliza) &
        (expuestos_periodos['AMPARO'] == amparo) &
        (expuestos_periodos['VIGENCIA_PERIODO_INICIO'] <= fecha_siniestro) &
        (expuestos_periodos['VIGENCIA_PERIODO_FIN'] >= fecha_siniestro)
    ]

    if len(periodos_match) == 0:
        # Siniestro sin período correspondiente
        siniestros_no_asignados.append(sin_row.to_dict())
    else:
        # Asignar el siniestro a TODOS los períodos que coinciden (solapamientos)
        for _, periodo_row in periodos_match.iterrows():
            registro = {
                **periodo_row.to_dict(),
                'DEPARTAMENTO_SINIESTRO': sin_row['DEPARTAMENTO_SINIESTRO'],
                'FECHA_DE_SINIESTRO': sin_row['FECHA_DE_SINIESTRO'],
                'FECHA_AVISO': sin_row['FECHA_AVISO'],
                'PAGOS': sin_row['PAGOS'],
                'RESERVA_ACTUAL_EQUI': sin_row['RESERVA_ACTUAL_EQUI'],
                'VALOR_ASEGURADO': sin_row['VALOR_ASEGURADO']
            }
            periodos_con_siniestro.append(registro)

print(f"  Procesados: {len(siniestros_consolidado):,} / {len(siniestros_consolidado):,}")

# Crear dataframe de siniestros no asignados
siniestros_no_asignados_df = pd.DataFrame(siniestros_no_asignados)

# Agregar períodos SIN siniestro al resultado final
print(f"\nAgregando períodos sin siniestro...")
periodos_sin_siniestro = []

for _, periodo_row in expuestos_periodos.iterrows():
    # Verificar si este período tiene siniestro
    tiene_siniestro = any(
        (p['ID_POLIZA_AMPARO_PERIODO'] == periodo_row['ID_POLIZA_AMPARO_PERIODO'])
        for p in periodos_con_siniestro
    )

    if not tiene_siniestro:
        registro = {
            **periodo_row.to_dict(),
            'DEPARTAMENTO_SINIESTRO': None,
            'FECHA_DE_SINIESTRO': None,
            'FECHA_AVISO': None,
            'PAGOS': 0.0,
            'RESERVA_ACTUAL_EQUI': 0.0,
            'VALOR_ASEGURADO': None
        }
        periodos_sin_siniestro.append(registro)

# Combinar períodos con y sin siniestro
base_completa = pd.DataFrame(periodos_con_siniestro + periodos_sin_siniestro)

print(f"\n" + "=" * 80)
print("RESULTADOS DEL JOIN TEMPORAL")
print("=" * 80)
print(f"\nSiniestros procesados: {len(siniestros_consolidado):,}")
print(f"Siniestros asignados a períodos: {len(periodos_con_siniestro):,}")
print(f"Siniestros NO asignados: {len(siniestros_no_asignados):,}")
print(f"\nPeríodos totales en base completa: {len(base_completa):,}")
print(f"  Con siniestro: {len(periodos_con_siniestro):,}")
print(f"  Sin siniestro: {len(periodos_sin_siniestro):,}")
```

## Cell 19: Validación de Asignación
```python
print("=" * 80)
print("VALIDACIÓN DE ASIGNACIÓN DE SINIESTROS")
print("=" * 80)

print(f"\nTotal de siniestros: {len(siniestros_consolidado):,}")
print(f"Asignados: {len(siniestros_consolidado) - len(siniestros_no_asignados):,}")
print(f"No asignados: {len(siniestros_no_asignados):,}")
print(f"Tasa de asignación: {((len(siniestros_consolidado) - len(siniestros_no_asignados)) / len(siniestros_consolidado) * 100):.2f}%")

if len(siniestros_no_asignados) > 0:
    print(f"\nPrimeros 5 siniestros no asignados:")
    for i, sin in enumerate(siniestros_no_asignados_df.head()['POLIZA']):
        print(f"  {i+1}. Póliza: {sin}")
```

## Cell 20: Filtrar Exposición Inválida
```python
# Filtrar registros con exposición válida
print(f"Registros antes de filtrar: {len(base_completa):,}")

base_filtrada = base_completa[
    (base_completa['EXPOSICION_PRORRATEADA'].notna()) &
    (base_completa['EXPOSICION_PRORRATEADA'] > 0)
].copy()

print(f"Registros después de filtrar: {len(base_filtrada):,}")
print(f"Registros eliminados: {len(base_completa) - len(base_filtrada):,}")
```

## Cell 21: Calcular Severidad Acotada
```python
# Calcular SEVERIDAD = min(max(RESERVA, PAGOS), EXPOSICION_PRORRATEADA)
base_filtrada['SEVERIDAD'] = np.minimum(
    np.maximum(
        base_filtrada['RESERVA_ACTUAL_EQUI'],
        base_filtrada['PAGOS']
    ),
    base_filtrada['EXPOSICION_PRORRATEADA']
)

print("Severidad calculada")
print(f"\nEstadísticas de SEVERIDAD:")
print(base_filtrada['SEVERIDAD'].describe())
```

## Cell 22: Calcular Severidad Extrapolada Anual (INNOVACIÓN DEL USUARIO)
```python
print("=" * 80)
print("EXTRAPOLACIÓN LINEAL A BASE ANUAL")
print("=" * 80)

# Extrapolar severidad a base anual para comparabilidad
base_filtrada['SEVERIDAD_EXTRAPOLADA_ANUAL'] = (
    base_filtrada['SEVERIDAD'] * (365.0 / base_filtrada['DIAS_EXPOSICION'])
)

print(f"\nSeveridad extrapolada calculada")
print(f"\nPeríodos con vigencia < 1 año y siniestro:")
periodos_cortos_siniestrados = base_filtrada[
    (base_filtrada['DIAS_EXPOSICION'] < 365) &
    (base_filtrada['SEVERIDAD'] > 0)
]
print(f"  Total: {len(periodos_cortos_siniestrados):,}")
if len(periodos_cortos_siniestrados) > 0:
    print(f"  Factor extrapolación promedio: {(365.0 / periodos_cortos_siniestrados['DIAS_EXPOSICION']).mean():.2f}x")
    print(f"  Severidad promedio original: ${periodos_cortos_siniestrados['SEVERIDAD'].mean():,.2f}")
    print(f"  Severidad promedio extrapolada: ${periodos_cortos_siniestrados['SEVERIDAD_EXTRAPOLADA_ANUAL'].mean():,.2f}")

print(f"\nEstadísticas de SEVERIDAD_EXTRAPOLADA_ANUAL:")
print(base_filtrada['SEVERIDAD_EXTRAPOLADA_ANUAL'].describe())
```

## Cell 23: Indicador TUVO_SINIESTRO
```python
# Crear indicador binario
base_filtrada['TUVO_SINIESTRO'] = (base_filtrada['SEVERIDAD'] > 0).astype(int)

print("Indicador TUVO_SINIESTRO creado")
print(f"\nDistribución:")
print(base_filtrada['TUVO_SINIESTRO'].value_counts())
print(f"\nTasa de siniestralidad: {base_filtrada['TUVO_SINIESTRO'].mean():.2%}")
```

## Cell 24: Selección de Columnas Finales
```python
# Seleccionar columnas finales
base_final = base_filtrada[[
    'ID_POLIZA_AMPARO_PERIODO',
    'POLIZA',
    'AMPARO',
    'ANIO_POLIZA',
    'PERIODO',
    'VIGENCIA_PERIODO_INICIO',
    'VIGENCIA_PERIODO_FIN',
    'DIAS_EXPOSICION',
    'FRACCION_ANUAL',
    'EXPOSICION_PRORRATEADA',
    'VIGENCIA_ORIGINAL_ID',
    'COD_SUCURSAL',
    'VIGENCIA_ORIGINAL_INICIO',
    'VIGENCIA_ORIGINAL_FIN',
    'DEPARTAMENTO_SINIESTRO',
    'FECHA_DE_SINIESTRO',
    'FECHA_AVISO',
    'SEVERIDAD',
    'SEVERIDAD_EXTRAPOLADA_ANUAL',
    'TUVO_SINIESTRO'
]].copy()

print(f"Columnas finales seleccionadas: {len(base_final.columns)}")
print(f"\nColumnas:")
for i, col in enumerate(base_final.columns, 1):
    print(f"  {i}. {col}")
```

## Cell 25: Validaciones Finales
```python
print("=" * 80)
print("VALIDACIONES FINALES")
print("=" * 80)

print(f"\n1. IDs únicos:")
print(f"   Total de registros: {len(base_final):,}")
print(f"   IDs únicos: {base_final['ID_POLIZA_AMPARO_PERIODO'].nunique():,}")
print(f"   ¿Hay duplicados?: {'SÍ' if base_final['ID_POLIZA_AMPARO_PERIODO'].nunique() < len(base_final) else 'NO'}")

print(f"\n2. Valores nulos en columnas críticas:")
print(f"   POLIZA: {base_final['POLIZA'].isna().sum():,}")
print(f"   AMPARO: {base_final['AMPARO'].isna().sum():,}")
print(f"   EXPOSICION_PRORRATEADA: {base_final['EXPOSICION_PRORRATEADA'].isna().sum():,}")
print(f"   SEVERIDAD: {base_final['SEVERIDAD'].isna().sum():,}")
print(f"   SEVERIDAD_EXTRAPOLADA_ANUAL: {base_final['SEVERIDAD_EXTRAPOLADA_ANUAL'].isna().sum():,}")

print(f"\n3. Rangos de valores:")
print(f"   Exposición prorrateada > 0: {(base_final['EXPOSICION_PRORRATEADA'] > 0).all()}")
print(f"   Severidad >= 0: {(base_final['SEVERIDAD'] >= 0).all()}")
print(f"   Severidad extrapolada >= 0: {(base_final['SEVERIDAD_EXTRAPOLADA_ANUAL'] >= 0).all()}")
```

## Cell 26: Resumen Estadístico Final
```python
print("=" * 80)
print("RESUMEN ESTADÍSTICO FINAL")
print("=" * 80)

print(f"\n1. DIMENSIONES")
print(f"   Registros totales: {len(base_final):,}")
print(f"   Pólizas únicas: {base_final['POLIZA'].nunique():,}")
print(f"   Combinaciones POLIZA-AMPARO únicas: {base_final.groupby(['POLIZA', 'AMPARO']).ngroups:,}")

print(f"\n2. DISTRIBUCIÓN TEMPORAL")
print(f"   Años únicos: {base_final['ANIO_POLIZA'].nunique()}")
print(f"   Rango de años: {base_final['ANIO_POLIZA'].min()} - {base_final['ANIO_POLIZA'].max()}")

print(f"\n3. EXPOSICIÓN")
print(f"   Total: ${base_final['EXPOSICION_PRORRATEADA'].sum():,.2f}")
print(f"   Promedio: ${base_final['EXPOSICION_PRORRATEADA'].mean():,.2f}")
print(f"   Mediana: ${base_final['EXPOSICION_PRORRATEADA'].median():,.2f}")

print(f"\n4. SINIESTRALIDAD")
print(f"   Registros con siniestro: {base_final['TUVO_SINIESTRO'].sum():,}")
print(f"   Registros sin siniestro: {(1 - base_final['TUVO_SINIESTRO']).sum():,}")
print(f"   Tasa de siniestralidad: {base_final['TUVO_SINIESTRO'].mean():.2%}")

print(f"\n5. SEVERIDAD (solo siniestrados)")
siniestrados = base_final[base_final['TUVO_SINIESTRO'] == 1]
if len(siniestrados) > 0:
    print(f"   Severidad promedio: ${siniestrados['SEVERIDAD'].mean():,.2f}")
    print(f"   Severidad mediana: ${siniestrados['SEVERIDAD'].median():,.2f}")
    print(f"   Severidad máxima: ${siniestrados['SEVERIDAD'].max():,.2f}")
    print(f"\n   Severidad extrapolada promedio: ${siniestrados['SEVERIDAD_EXTRAPOLADA_ANUAL'].mean():,.2f}")
    print(f"   Severidad extrapolada mediana: ${siniestrados['SEVERIDAD_EXTRAPOLADA_ANUAL'].median():,.2f}")
    print(f"   Severidad extrapolada máxima: ${siniestrados['SEVERIDAD_EXTRAPOLADA_ANUAL'].max():,.2f}")

print(f"\n6. VIGENCIAS CORTAS")
vigencias_cortas_final = base_final[base_final['DIAS_EXPOSICION'] < 365]
print(f"   Total períodos < 365 días: {len(vigencias_cortas_final):,} ({100*len(vigencias_cortas_final)/len(base_final):.2f}%)")
vigencias_cortas_siniestradas = vigencias_cortas_final[vigencias_cortas_final['TUVO_SINIESTRO'] == 1]
print(f"   Con siniestro: {len(vigencias_cortas_siniestradas):,}")
if len(vigencias_cortas_siniestradas) > 0:
    print(f"   Factor extrapolación promedio: {(365.0 / vigencias_cortas_siniestradas['DIAS_EXPOSICION']).mean():.2f}x")
```

## Cell 27: Exportar Resultados
```python
print("=" * 80)
print("EXPORTANDO RESULTADOS")
print("=" * 80)

# Exportar base final
base_final.to_csv('../data/tmp/base_final_anual.csv', index=False)
print(f"\n✅ base_final_anual.csv exportado ({len(base_final):,} registros)")

# Exportar siniestros no asignados
if len(siniestros_no_asignados) > 0:
    siniestros_no_asignados_df.to_csv('../data/tmp/siniestros_no_asignados.csv', index=False)
    print(f"✅ siniestros_no_asignados.csv exportado ({len(siniestros_no_asignados):,} registros)")

print(f"\n" + "=" * 80)
print("TRANSFORMACIÓN COMPLETADA")
print("=" * 80)
print(f"\nArchivos generados:")
print(f"  1. base_final_anual.csv - Base principal para análisis Bühlmann-Straub")
print(f"  2. siniestros_no_asignados.csv - Siniestros sin vigencias correspondientes")
```
