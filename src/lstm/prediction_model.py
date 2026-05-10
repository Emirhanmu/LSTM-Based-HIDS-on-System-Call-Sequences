import torch
import torch.nn as nn


class LSTMNextSyscallPredictor(nn.Module):
    def __init__(
        self,
        vocab_size: int,
        embedding_dim: int = 64,
        hidden_dim: int = 128,
        num_layers: int = 1,
        dropout: float = 0.2,
        pad_idx: int = 0,
    ):
        super().__init__()

        self.embedding = nn.Embedding(
            num_embeddings=vocab_size,
            embedding_dim=embedding_dim,
            padding_idx=pad_idx,
        )

        self.lstm = nn.LSTM(
            input_size=embedding_dim,
            hidden_size=hidden_dim,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )

        self.dropout = nn.Dropout(dropout)
        self.output_layer = nn.Linear(hidden_dim, vocab_size)

    def forward(self, input_ids):
        # input_ids: [batch_size, seq_len]
        embedded = self.embedding(input_ids)  # [B, L, E]

        _, (hidden, _) = self.lstm(embedded)

        final_hidden = hidden[-1]  # [B, H]
        final_hidden = self.dropout(final_hidden)

        logits = self.output_layer(final_hidden)  # [B, vocab_size]
        return logits


if __name__ == "__main__":
    batch_size = 4
    seq_len = 29
    vocab_size = 61

    model = LSTMNextSyscallPredictor(
        vocab_size=vocab_size,
        embedding_dim=64,
        hidden_dim=128,
        num_layers=1,
        dropout=0.2,
        pad_idx=0,
    )

    sample_input = torch.randint(0, vocab_size, (batch_size, seq_len))
    output = model(sample_input)

    print("Input shape:", sample_input.shape)
    print("Output shape:", output.shape)
    print("Output:", output[:2, :5])