import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

source_csv_path = BASE_DIR / 'dec8_dedupe_data.csv'
source_csv_labels_save_path =  BASE_DIR / 'labeled_real_datat.csv'
synth_data_save_path = BASE_DIR / 'synth_labeled_data.csv'
df = pd.read_csv(source_csv_path)
features = ['AVG_SPEED', 'MAX_SPEED', 'MIN_SPEED', 'ACCELERATION', 
            'SPEED_VARIANCE', 'YAW_VARIANCE', 'PITCH_VARIANCE', 'ROLL_VARIANCE']

#determine if driving is risky by summing nomralized features, and we label top 30% streams with highest scores as risky
stream_stats = df.groupby('STREAM_KEY')[features].max()

norm_stats = (stream_stats - stream_stats.min()) / (stream_stats.max() - stream_stats.min())

risk_score = norm_stats.sum(axis=1)

#Lable top30% of streams as risky
threshold = risk_score.quantile(0.7)
labels = (risk_score > threshold).astype(int)

labeled_map = pd.DataFrame({'STREAM_KEY': stream_stats.index, 'LABEL': labels}).reset_index(drop=True)
df_labeled = df.merge(labeled_map, on='STREAM_KEY')
df_labeled.to_csv(source_csv_labels_save_path, index=False)
print("Saved labeled real data.")

# Syntehtic data gen using safe and risky realy data's mean and variance
safe_dist = df_labeled[df_labeled['LABEL'] == 0][features]
risky_dist = df_labeled[df_labeled['LABEL'] == 1][features]

safe_mean, safe_cov = safe_dist.mean(), safe_dist.cov()
risky_mean, risky_cov = risky_dist.mean(), risky_dist.cov()

def generate_stream(stream_id, label, n_windows):
    mean, cov = (safe_mean, safe_cov) if label == 0 else (risky_mean, risky_cov)
    
    try:
        data = np.random.multivariate_normal(mean, cov, n_windows)
    except:
        #fallback for singular matrix
        data = np.random.multivariate_normal(mean, np.diag(np.diag(cov)), n_windows)
        
    syn_df = pd.DataFrame(data, columns=features)
    
    for col in features:
        if 'VARIANCE' in col or 'ACCELERATION' in col:
            syn_df[col] = syn_df[col].abs()
            
    syn_df['MAX_SPEED'] = np.maximum(syn_df['AVG_SPEED'], syn_df['MAX_SPEED'])
    syn_df['MIN_SPEED'] = np.minimum(syn_df['AVG_SPEED'], syn_df['MIN_SPEED'])
    
    syn_df['STREAM_KEY'] = f"syn_8feat_{stream_id}"
    syn_df['LABEL'] = label
    syn_df['WINDOW_START'] = pd.date_range(start='2025-01-01', periods=n_windows, freq='10s')
    syn_df['WINDOW_END'] = syn_df['WINDOW_START'] + pd.Timedelta(seconds=10)
    
    return syn_df

#stream gen 200 streams
syn_data = []
for i in range(200):
    length = np.random.randint(20, 101) #simulate variable length of streams
    syn_data.append(generate_stream(i, i % 2, length))

final_syn_df = pd.concat(syn_data)
cols = ['STREAM_KEY', 'LABEL', 'WINDOW_START', 'WINDOW_END'] + features
final_syn_df = final_syn_df[cols]
final_syn_df.to_csv(synth_data_save_path, index=False)
print("Saved synthetic data.")