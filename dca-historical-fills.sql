with
executed_volume AS (
    SELECT 
        call_block_time,
        case when output_mint = '{{token}}' then out_amount / POWER(10, tk_out.decimals) else 0 end as buy_amount,
        case when output_mint = '{{token}}' then -in_amount / POWER(10, tk_out.decimals) else 0 end as sell_amount
    FROM dune.dune.result_jupiter_dca_fill_volume_mat v
    -- tk_in, tk_out for decimals
    LEFT JOIN tokens_solana.fungible tk_in ON tk_in.token_mint_address = v.input_mint
    LEFT JOIN tokens_solana.fungible tk_out ON tk_out.token_mint_address = v.output_mint
    WHERE (input_mint = '{{token}}' OR output_mint = '{{token}}')
)

select * from executed_volume
order by call_block_time desc