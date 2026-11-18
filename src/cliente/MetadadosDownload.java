package cliente;

public record MetadadosDownload (int idTransferencia, String nome, long tamanhoArquivo, int numeroFragmentos, String hash) {}