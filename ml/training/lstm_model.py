from torch import nn
import torch


class LSTMAutoEncoder(nn.Module):
    """
    LSTMAutoEncoder is a PyTorch module that implements an LSTM-based autoencoder architecture for sequence reconstruction.
    The encoder compresses the input sequence into a hidden representation, and the decoder tries to reconstruct
    the original sequence from this hidden representation. The reconstruction error can be used as an anomaly score for time series data.
    """

    def __init__(
        self,
        n_features: int,
        hidden_dim: int,
        n_layers: int,
        dropout: float,
    ) -> None:
        super().__init__()

        # The encoder LSTM compresses the input sequence into a hidden representation of size hidden_dim.
        # Encoder compresses and sends to final hidden state.
        self.encoder = nn.LSTM(
            input_size=n_features,
            hidden_size=hidden_dim,
            num_layers=n_layers,
            dropout=dropout if n_layers > 1 else 0.0,
            batch_first=True,
        )

        # The decoder LSTM tries to reconstruct the original sequence from this hidden representation.
        # Decoder reconstructs the sequence from the final hidden state of the encoder, repeated across the sequence length.
        self.decoder = nn.LSTM(
            input_size=hidden_dim,
            hidden_size=n_features,
            num_layers=n_layers,
            dropout=dropout if n_layers > 1 else 0.0,
            batch_first=True,
        )

        # A linear layer to project the decoder's output back to the original feature space.
        self.output_proj = nn.Linear(hidden_dim, n_features)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """
        Forward pass through the LSTM Autoencoder.
        The input x has shape (batch_size, seq_len, n_features). The output has
        the same shape, representing the reconstruction of the input sequence.
        The encoder processes the input sequence and returns the final hidden state,
        which is then repeated across the sequence length and fed into the decoder to reconstruct the original sequence.

        Args:
            x: Input tensor of shape (batch_size, seq_len, n_features)

        Returns:
            Reconstructed tensor of shape (batch_size, seq_len, n_features)
        """
        # Encode the input sequence into the hidden representation
        _, seq_len, _ = x.shape

        # Encode: The encoder processes the input sequence and returns the final hidden state (h_n, c_n).
        # h_n: (n_layers, batch_size, hidden_dim)
        # c_n: (n_layers, batch_size, hidden_dim)
        _, (h_n, c_n) = self.encoder(x)

        # Expand the final hidden state across the sequence length to create the decoder input
        # Take the last layer's hidden state and repeat it seq_len times to match the input sequence length
        # context: (batch_size, hidden_dim) -> (batch_size, seq_len, hidden_dim)
        context = h_n[-1].unsqueeze(1).expand(-1, seq_len, -1)

        # Decode: The decoder takes the repeated hidden state as input and tries to reconstruct the original sequence
        # dec_out: (batch_size, seq_len, hidden_dim)
        dec_out, _ = self.decoder(context, (h_n, c_n))

        # Project back to original feature space
        return self.output_proj(dec_out)

    def reconstruction_error(self, x: torch.Tensor) -> torch.Tensor:
        """
        Compute the mean squared error between the input sequence and its reconstruction.
        The error is averaged across the feature and sequence dimensions, resulting in a single error score per
        sequence in the batch.

        Args:
            x: Input tensor of shape (batch_size, seq_len, n_features)

        Returns:
            Tensor of shape (batch_size,) containing the reconstruction error for each sequence in the batch.
        """
        recon = self.forward(x)

        # Compute mean squared error across the feature and sequence dimensions,
        # resulting in a single error score per sequence in the batch
        return torch.mean((x - recon) ** 2, dim=(1, 2))
