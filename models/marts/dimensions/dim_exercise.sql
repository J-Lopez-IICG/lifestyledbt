WITH stg AS (
    -- 1. Obtenemos los atributos únicos por ejercicio
    SELECT DISTINCT
        Nombre_Ejercicio,
        Beneficios,
        Quema_Calorias_por_30_min,
        Quema_Calorias_por_30_min_bc,
        Quema_Calorias_Bin,
        Entrenamiento,
        
        -- Campos de enlace para las sub-dimensiones
        Equipo_Necesario,
        Nivel_Dificultad,
        Grupo_Muscular_Objetivo,
        Tipo_Musculo,
        Parte_Cuerpo
    FROM 
        {{ ref('stg_lifestyle') }}
    WHERE 
        Nombre_Ejercicio IS NOT NULL
),

-- 2. Traemos las dimensiones "copo de nieve"
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

-- 3. Unimos todo para crear la dimensión "padre"
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