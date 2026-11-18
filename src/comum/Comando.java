package comum;

/**
 * Comandos do protocolo do servidor de arquivos UDP.
 *
 * O datagrama transporta o comando como 1 byte imediatamente após a versão
 * do protocolo (ver Datagrama.java). Os códigos foram atribuídos em ordem
 * crescente para facilitar depuração e leitura do log.
 *
 * ACK não aparece como comando na tabela original do protocolo, mas é a
 * resposta positiva padrão indicada em "Resposta" para REGISTER, UPLOAD e
 * DATA. Ele foi incluído aqui para que Datagrama consiga serializar/parsear
 * confirmações sem depender de um formato paralelo.
 */
public enum Comando {
    REGISTER((byte) 1),
    LIST((byte) 2),
    UPLOAD((byte) 3),
    DATA((byte) 4),
    DOWNLOAD((byte) 5),
    UPDATE_LIST((byte) 6),
    ACK((byte) 7),
    ERROR((byte) 8);

    private final byte codigo;

    Comando(byte codigo) {
        this.codigo = codigo;
    }

    public byte getCodigo() {
        return codigo;
    }

    public static Comando fromCodigo(byte codigo) {
        for (Comando comando : values()) {
            if (comando.codigo == codigo) {
                return comando;
            }
        }
        throw new IllegalArgumentException("Código de comando desconhecido: " + codigo);
    }
}