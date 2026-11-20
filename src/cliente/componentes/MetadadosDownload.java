package cliente.componentes;

public record MetadadosDownload (int idTransferencia, String nome, long tamanhoArquivo, int numeroFragmentos, String hash) {}