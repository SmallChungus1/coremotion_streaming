
#note: aten::_nested_tensor_from_mask_left_aligned not implemented for MPS so we need to fallback to CPU for this op
#SO BEFORE RUNNING THIS SCRIPT: export PYTORCH_ENABLE_MPS_FALLBACK=1

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader, random_split
from torch.nn.utils.rnn import pad_sequence
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from pathlib import Path
from torch.utils.data import ConcatDataset
import os 
import time

matplotlib.use('Agg') #use agg backend to prevent rendering UI

BASE_DIR = Path(__file__).resolve().parent

if torch.backends.mps.is_available():
    device = torch.device("mps")
    print("Using MPS.")
elif torch.cuda.is_available():
    device = torch.device("cuda")
    print("Using CUDA.")
else:
    device = torch.device("cpu")
    print("Using CPU.")

class DrivingDataset(Dataset):
    def __init__(self, csv_file):
        df = pd.read_csv(csv_file)
        self.data = []
        self.labels = []
        
        #1 sample is 1 entire stream session, which has varied amount of samples
        grouped = df.groupby('STREAM_KEY')
        feature_cols = ['AVG_SPEED', 'MAX_SPEED', 'MIN_SPEED', 'ACCELERATION', 
                        'SPEED_VARIANCE', 'YAW_VARIANCE', 'PITCH_VARIANCE', 'ROLL_VARIANCE']
        
        for name, group in grouped:
            features = group[feature_cols].values.astype(np.float32)
            label = group['LABEL'].iloc[0]
            
            self.data.append(torch.tensor(features))
            self.labels.append(torch.tensor(label, dtype=torch.long))
            
    def __len__(self):
        return len(self.data)
    
    def __getitem__(self, idx):
        return self.data[idx], self.labels[idx]

#helper to pad variable length sequences by padding longest in the batch
def collate_fn(batch):
    sequences, labels = zip(*batch)
    
    padded_sequences = pad_sequence(sequences, batch_first=True, padding_value=0.0)
    
    #creating a mask where true when index >= length
    lengths = torch.tensor([len(s) for s in sequences])
    padding_mask = torch.arange(padded_sequences.size(1))[None, :] >= lengths[:, None]
    
    labels = torch.stack(labels)
    return padded_sequences, labels, padding_mask

class DrivingTransformer(nn.Module):
    def __init__(self, input_dim=8, d_model=64, nhead=4, num_layers=2, num_classes=2):
        super(DrivingTransformer, self).__init__()
        
        self.embedding = nn.Linear(input_dim, d_model)
        self.pos_encoder = nn.Parameter(torch.randn(1, 500, d_model))
        
        encoder_layer = nn.TransformerEncoderLayer(d_model=d_model, nhead=nhead, batch_first=True)
        self.transformer_encoder = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)
        
        self.mlp_head = nn.Sequential(
            nn.Linear(d_model, 32),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(32, num_classes)
        )
        
    def forward(self, x, src_key_padding_mask=None):
        x = self.embedding(x)
        seq_len = x.size(1)
        x = x + self.pos_encoder[:, :seq_len, :]
        
        transformer_out = self.transformer_encoder(x, src_key_padding_mask=src_key_padding_mask)
        
        #mean Pooling (ignoring padding)
        if src_key_padding_mask is not None:
            mask = ~src_key_padding_mask #invert so True = Valid Data
            mask = mask.unsqueeze(-1).float() 
            sum_embeddings = (transformer_out * mask).sum(dim=1)
            count = mask.sum(dim=1).clamp(min=1e-9)
            pooled = sum_embeddings / count
        else:
            pooled = transformer_out.mean(dim=1)
            
        return self.mlp_head(pooled)

### TRAININ SETUP
def train_model():
    synth_dataset = DrivingDataset(BASE_DIR / 'synth_labeled_data.csv')
    real_dataset = DrivingDataset(BASE_DIR / 'labeled_real_data.csv')

    dataset = ConcatDataset([synth_dataset, real_dataset])


    train_size = int(0.8 * len(dataset))
    val_size = len(dataset) - train_size
    train_dataset, val_dataset = random_split(dataset, [train_size, val_size])

    train_loader = DataLoader(train_dataset, batch_size=16, shuffle=True, collate_fn=collate_fn)
    val_loader = DataLoader(val_dataset, batch_size=16, shuffle=False, collate_fn=collate_fn)


    model = DrivingTransformer().to(device)
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)

    #Trai/val loops

    num_epochs = 100
    train_losses = []
    val_losses = []
    best_val_loss = float('inf')
    early_stop_patience = 15

    print(f"Starting training for {num_epochs} epochs...")

    time_start = time.perf_counter()
    for epoch in range(num_epochs):
        model.train()
        running_train_loss = 0.0
        
        for inputs, labels, mask in train_loader:
            inputs, labels, mask = inputs.to(device), labels.to(device), mask.to(device)
            
            optimizer.zero_grad()
            outputs = model(inputs, src_key_padding_mask=mask)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            
            running_train_loss += loss.item() * inputs.size(0)
        
        epoch_train_loss = running_train_loss / len(train_dataset)
        train_losses.append(epoch_train_loss)
        
        model.eval()
        running_val_loss = 0.0
        correct = 0
        total = 0
        
        with torch.no_grad():
            for inputs, labels, mask in val_loader:
                inputs, labels, mask = inputs.to(device), labels.to(device), mask.to(device)
                
                outputs = model(inputs, src_key_padding_mask=mask)
                loss = criterion(outputs, labels)
                
                running_val_loss += loss.item() * inputs.size(0)
                _, predicted = torch.max(outputs.data, 1)
                total += labels.size(0)
                correct += (predicted == labels).sum().item()

        epoch_val_loss = running_val_loss / len(val_dataset)
        val_losses.append(epoch_val_loss)
        val_acc = 100 * correct / total

        if epoch_val_loss < best_val_loss:
            best_val_loss = epoch_val_loss
            no_improvment_tally = 0
            #clear old weights first, then save new one
            for f_name in os.listdir(BASE_DIR):
                if ".pth" in f_name:
                    os.remove(BASE_DIR / f_name)

            torch.save(model.state_dict(), BASE_DIR / f"transformer_drive_cls_ep{epoch}.pth")
        else: 
            no_improvment_tally += 1
        
        print(f"Epoch {epoch+1:02d}/{num_epochs} | Train Loss: {epoch_train_loss:.4f} | Val Loss: {epoch_val_loss:.4f} | Val Acc: {val_acc:.2f}%")

        if no_improvment_tally > early_stop_patience:
            print(f"Training early stopped at epoch {epoch}")
            break



    time_end = time.perf_counter()

    print(f"Total training time: {time_end-time_start:.2f} seconds")


    #train/val chart
    plt.figure(figsize=(10, 6))
    plt.plot(train_losses, label='Training Loss', marker='o')
    plt.plot(val_losses, label='Validation Loss', marker='o')
    plt.title('Transformer Training Progress')
    plt.xlabel('Epochs')
    plt.ylabel('Cross Entropy Loss')
    plt.legend()
    plt.grid(True)
    plt.savefig(BASE_DIR / 'training_loss_chart.png')
    print(f"Chart saved to {BASE_DIR / 'training_loss_chart.png'}")


if __name__ == "__main__":
    train_model()