WITH stg AS (
    -- 1. Agrupar por la llave de negocio (Nombre_Comida)
    SELECT 
        Nombre_Comida,
        ANY_VALUE(Tiempo_Preparacion_Minutos) AS Tiempo_Preparacion_Minutos,
        ANY_VALUE(Tiempo_Coccion_Minutos) AS Tiempo_Coccion_Minutos,
        ANY_VALUE(Calificacion) AS Calificacion_Comida,
        
        -- Campos de enlace para las sub-dimensiones
        ANY_VALUE(Tipo_Comida) AS Tipo_Comida,
        ANY_VALUE(Tipo_Dieta) AS Tipo_Dieta,
        ANY_VALUE(Metodo_Coccion) AS Metodo_Coccion
    FROM 
        {{ ref('stg_lifestyle') }}
    WHERE 
        Nombre_Comida IS NOT NULL
    GROUP BY
        Nombre_Comida
),

-- 2. Traemos las dimensiones "copo de nieve" (esto no cambia)
dim_meal_type AS (
    SELECT * FROM {{ ref('dim_meal_type') }}
),
dim_diet_type AS (
    SELECT * FROM {{ ref('dim_diet_type') }}
),
dim_cooking_method AS (
    SELECT * FROM {{ ref('dim_cooking_method') }}
)

-- 3. Unimos todo (esto no cambia)
SELECT
    {{ dbt_utils.generate_surrogate_key(['stg.Nombre_Comida']) }} AS dim_meal_key,
    stg.Nombre_Comida,
    stg.Tiempo_Preparacion_Minutos,
    stg.Tiempo_Coccion_Minutos,
    stg.Calificacion_Comida,

    -- Estas son las llaves foráneas que crean el "copo de nieve"
    dim_meal_type.dim_meal_type_key,
    dim_diet_type.dim_diet_type_key,
    dim_cooking_method.dim_cooking_method_key

FROM stg
LEFT JOIN dim_meal_type 
    ON stg.Tipo_Comida = dim_meal_type.Tipo_Comida
LEFT JOIN dim_diet_type 
    ON stg.Tipo_Dieta = dim_diet_type.Tipo_Dieta
LEFT JOIN dim_cooking_method 
    ON stg.Metodo_Coccion = dim_cooking_method.Metodo_Coccion