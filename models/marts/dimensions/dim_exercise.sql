WITH stg AS (
    -- 1. Agrupar por la llave de negocio (Nombre_Ejercicio)
    --    y tomar un solo valor (ANY_VALUE) para sus atributos.
    SELECT 
        Nombre_Ejercicio,
        ANY_VALUE(Beneficios) AS Beneficios,
        ANY_VALUE(Quema_Calorias_por_30_min) AS Quema_Calorias_por_30_min,
        ANY_VALUE(Quema_Calorias_por_30_min_bc) AS Quema_Calorias_por_30_min_bc,
        ANY_VALUE(Quema_Calorias_Bin) AS Quema_Calorias_Bin,
        ANY_VALUE(Entrenamiento) AS Entrenamiento,
        
        -- Campos de enlace para las sub-dimensiones
        ANY_VALUE(Equipo_Necesario) AS Equipo_Necesario,
        ANY_VALUE(Nivel_Dificultad) AS Nivel_Dificultad,
        ANY_VALUE(Grupo_Muscular_Objetivo) AS Grupo_Muscular_Objetivo,
        ANY_VALUE(Tipo_Musculo) AS Tipo_Musculo,
        ANY_VALUE(Parte_Cuerpo) AS Parte_Cuerpo
    FROM 
        {{ ref('stg_lifestyle') }}
    WHERE 
        Nombre_Ejercicio IS NOT NULL
    GROUP BY
        Nombre_Ejercicio
),

-- 2. Traemos las dimensiones "copo de nieve" (esto no cambia)
dim_equipment AS (
    SELECT * FROM {{ ref('dim_equipment') }}
),
dim_difficulty AS (
    SELECT * FROM {{ ref('dim_difficulty') }}
),
dim_muscle_group AS (
    SELECT * FROM {{ ref('dim_muscle_group') }}
),
dim_body_part AS (
    SELECT * FROM {{ ref('dim_body_part') }}
)

-- 3. Unimos todo (esto no cambia)
SELECT
    {{ dbt_utils.generate_surrogate_key(['stg.Nombre_Ejercicio']) }} AS dim_exercise_key,
    stg.Nombre_Ejercicio,
    stg.Beneficios,
    stg.Entrenamiento,
    stg.Quema_Calorias_por_30_min,
    stg.Quema_Calorias_por_30_min_bc,
    stg.Quema_Calorias_Bin,
    
    -- Estas son las llaves foráneas que crean el "copo de nieve"
    dim_equipment.dim_equipment_key,
    dim_difficulty.dim_difficulty_key,
    dim_muscle_group.dim_muscle_group_key,
    dim_body_part.dim_body_part_key

FROM stg
LEFT JOIN dim_equipment 
    ON stg.Equipo_Necesario = dim_equipment.Equipo_Necesario
LEFT JOIN dim_difficulty 
    ON stg.Nivel_Dificultad = dim_difficulty.Nivel_Dificultad
LEFT JOIN dim_muscle_group 
    ON stg.Grupo_Muscular_Objetivo = dim_muscle_group.Grupo_Muscular_Objetivo
    AND stg.Tipo_Musculo = dim_muscle_group.Tipo_Musculo
LEFT JOIN dim_body_part 
    ON stg.Parte_Cuerpo = dim_body_part.Parte_Cuerpo