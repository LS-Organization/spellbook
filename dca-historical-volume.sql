WITH date_range AS (
    SELECT date_trunc('minute', CAST('{{end_date}}' AS timestamp)) AS end_date,
           date_trunc('minute', CAST('{{end_date}}' AS timestamp) - INTERVAL '{{days_to_look_back}}' day) AS start_date
),
time_series AS (
    SELECT date_add('minute', -n, end_date) AS date
    FROM date_range,
         UNNEST(SEQUENCE(0, DATE_DIFF('minute', start_date, end_date))) AS t(n)
),
executed_volume AS (
    SELECT 
        date_trunc('minute', call_block_time) AS date,
        SUM(CASE 
            WHEN output_mint = '{{token}}'
            THEN -out_amount / POWER(10, tk_out.decimals)
            ELSE in_amount / POWER(10, tk_in.decimals)
        END) AS net_volume
    FROM dune.dune.result_jupiter_dca_fill_volume_mat v
    LEFT JOIN tokens_solana.fungible tk_in ON tk_in.token_mint_address = v.input_mint
    LEFT JOIN tokens_solana.fungible tk_out ON tk_out.token_mint_address = v.output_mint
    WHERE (input_mint = '{{token}}' OR output_mint = '{{token}}')
      AND call_block_time >= (SELECT start_date FROM date_range)
    GROUP BY 1
),
open_positions AS (
    SELECT 
        date_trunc('minute', dca.call_block_time) AS open_date,
        COALESCE(date_trunc('minute', LEAST(c_dca.call_block_time, c_dca2.call_block_time)), CURRENT_TIMESTAMP) AS close_date,
        CASE 
            WHEN dca.account_outputMint = '{{token}}'
            THEN CAST(dca.inAmount AS double) / POWER(10, COALESCE(tk_in.decimals, tk_out.decimals))
            ELSE -CAST(dca.inAmount AS double) / POWER(10, COALESCE(tk_in.decimals, tk_out.decimals))
        END AS total_volume,
        CASE 
            WHEN dca.account_outputMint = '{{token}}'
            THEN (CAST(dca.inAmount AS double) / POWER(10, COALESCE(tk_in.decimals, tk_out.decimals))) / (DATE_DIFF('minute', dca.call_block_time, COALESCE(date_trunc('minute', LEAST(c_dca.call_block_time, c_dca2.call_block_time)), CURRENT_TIMESTAMP)))
            ELSE -(CAST(dca.inAmount AS double) / POWER(10, COALESCE(tk_in.decimals, tk_out.decimals))) / (DATE_DIFF('minute', dca.call_block_time, COALESCE(date_trunc('minute', LEAST(c_dca.call_block_time, c_dca2.call_block_time)), CURRENT_TIMESTAMP)))
        END AS volume_per_minute
    FROM (
        SELECT 
            call_block_time,
            account_dca,
            account_inputMint,
            account_outputMint,
            CAST(inAmount AS varchar) AS inAmount,
            CAST(inAmountPerCycle AS varchar) AS inAmountPerCycle
        FROM dca_solana.dca_call_openDcaV2
        WHERE call_block_time >= (SELECT start_date FROM date_range)
        AND maxPrice = 0 AND minPrice = 0
    ) dca
    LEFT JOIN dca_solana.dca_call_closeDca c_dca ON c_dca.account_dca = dca.account_dca
    LEFT JOIN dca_solana.dca_call_endAndClose c_dca2 ON c_dca2.account_dca = dca.account_dca
    LEFT JOIN tokens_solana.fungible tk_in ON tk_in.token_mint_address = dca.account_inputMint
    LEFT JOIN tokens_solana.fungible tk_out ON tk_out.token_mint_address = dca.account_outputMint
    WHERE dca.account_inputMint = '{{token}}' OR dca.account_outputMint = '{{token}}'
),
cumulative_volume AS (
    SELECT 
        date,
        SUM(COALESCE(net_volume, 0)) OVER (ORDER BY date DESC) AS cumulative_past_volume
    FROM (
        SELECT ts.date, COALESCE(ev.net_volume, 0) AS net_volume
        FROM time_series ts
        LEFT JOIN executed_volume ev ON ev.date = ts.date
    ) daily_volumes
)

SELECT 
    ts.date,
    cv.cumulative_past_volume,
    SUM(
        CASE 
            WHEN op.open_date <= ts.date AND op.close_date > ts.date 
            THEN (DATE_DIFF('minute', ts.date, op.close_date) * op.volume_per_minute)
            ELSE 0 
        END
    ) AS remaining_future_volume
FROM time_series ts
LEFT JOIN cumulative_volume cv ON cv.date = ts.date
LEFT JOIN open_positions op ON ts.date BETWEEN op.open_date AND op.close_date
GROUP BY 1, 2
ORDER BY 1 DESC
LIMIT 9000  -- Adjust this limit as needed