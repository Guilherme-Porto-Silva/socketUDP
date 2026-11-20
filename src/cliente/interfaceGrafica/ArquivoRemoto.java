package cliente.interfaceGrafica;

/**
 * Arquivo listado pelo servidor.
 *
 * @param nome    nome do arquivo no servidor
 * @param tamanho tamanho em bytes, ou -1 quando o servidor não informa
 */
public record ArquivoRemoto (String nome, long tamanho) {

    @Override public String toString () { return nome; }
}