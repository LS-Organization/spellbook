/*
Returns the latest list of all DCA accounts and their live status.
*/

WITH dca_positions AS (
    SELECT 
        account_dca,
        get_href(get_chain_explorer('solana') || '/account/' || account_inputMint, COALESCE(tk_in.symbol, account_inputMint)) as sold_token,
        get_href(get_chain_explorer('solana') || '/account/' || account_outputMint, COALESCE(tk_out.symbol, account_outputMint)) as bought_token,
        CASE 
            WHEN cycleFrequency >= 2419200 THEN 'month'
            WHEN cycleFrequency >= 604800 THEN 'week'
            WHEN cycleFrequency >= 86400 THEN 'day'
            WHEN cycleFrequency >= 3600 THEN 'hour'
            WHEN cycleFrequency >= 60 THEN 'minute'
        END AS frequency,
        cycleFrequency AS frequency_seconds,
        inAmount/inAmountPerCycle AS cycles,
        inAmount/pow(10, tk_in.decimals) AS in_amount_total,
        inAmountPerCycle/pow(10, tk_in.decimals) AS in_amount_per,
        call_tx_id,
        call_block_time AS open_block_time,
        account_inputMint AS input_mint,
        account_outputMint AS output_mint,
        account_user
    FROM dca_solana.dca_call_openDcaV2 dca -- note we ignore the old openDca version
    LEFT JOIN tokens_solana.fungible tk_in ON tk_in.token_mint_address = account_inputMint
    LEFT JOIN tokens_solana.fungible tk_out ON tk_out.token_mint_address = account_outputMint
),

filled_amounts AS (
    SELECT 
        account_dca,
        SUM(in_amount/pow(10,tk.decimals)) AS input_amount_filled,
        COUNT(*) AS cycles_passed
    FROM dune.dune.result_jupiter_dca_fill_volume_mat v
    LEFT JOIN tokens_solana.fungible tk ON tk.token_mint_address = v.input_mint
    GROUP BY 1
)

SELECT 
    CASE 
        WHEN COALESCE(fills.input_amount_filled,0) < dca.in_amount_total 
            AND c_dca.call_tx_id IS NULL AND c_dca2.call_tx_id IS NULL THEN '🟢 Open'
        ELSE '🔴 Closed' 
    END AS state,
    get_href(get_chain_explorer('solana') || '/account/' || dca.account_user,
        COALESCE('✔️ ' || COALESCE(sns.favorite_domain, TRY(sns.domains_owned[1])), SUBSTRING(dca.account_user,1,6) || '...')) 
        AS account_user,
    dca.account_dca,
    get_href(get_chain_explorer('solana') || '/account/' || dca.account_dca, 'DCA 🔗') AS dca,
    dca.sold_token,
    dca.bought_token,
    dca.frequency,
    dca.frequency_seconds,
    dca.cycles,
    fills.cycles_passed,
    CAST(COALESCE(fills.input_amount_filled,0) AS DOUBLE)/CAST(dca.in_amount_total AS DOUBLE) AS in_amount_fill_percentage,
    dca.in_amount_total*p.price AS in_amount_total_usd,
    fills.input_amount_filled*p.price AS input_amount_filled_usd,
    dca.in_amount_total,
    fills.input_amount_filled,
    dca.in_amount_per,
    dca.in_amount_per*p.price AS in_amount_per_usd,
    dca.in_amount_total - COALESCE(fills.input_amount_filled,0) AS in_amount_left,
    (dca.in_amount_total - COALESCE(fills.input_amount_filled,0))*p.price AS in_amount_left_usd,
    COALESCE(c_dca.call_tx_id, c_dca2.call_tx_id) AS close_tx,
    dca.call_tx_id AS open_tx,
    dca.open_block_time,
    COALESCE(c_dca.call_block_time, c_dca2.call_block_time) AS close_block_time,
    dca.input_mint,
    dca.output_mint
FROM dca_positions dca
LEFT JOIN solana_utils.sns_domains sns ON sns.owner = dca.account_user
LEFT JOIN filled_amounts fills ON fills.account_dca = dca.account_dca
LEFT JOIN dca_solana.dca_call_closeDca c_dca ON c_dca.account_dca = dca.account_dca
LEFT JOIN dca_solana.dca_call_endAndClose c_dca2 ON c_dca2.account_dca = dca.account_dca
LEFT JOIN prices.usd_latest p ON p.blockchain = 'solana' 
    AND toBase58(p.contract_address) = dca.input_mint