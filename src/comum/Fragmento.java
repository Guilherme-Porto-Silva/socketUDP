package comum;

import java.util.Arrays;

/**
 * Representa um fragmento de arquivo transferido via UDP.
 *
 * Cada fragmento carrega:
 * - idTransferencia: identifica a transferência à qual o fragmento pertence;
 * - numeroFragmento: posição sequencial do fragmento no arquivo original;
 * - payload: bytes do pedaço do arquivo.
 *
 * O payload é limitado a TAMANHO_MAXIMO (1024 bytes), conforme a decisão de
 * projeto: o fragmento cabe no MTU Ethernet de 1500 bytes, reduzindo perda de
 * pacotes e fragmentação na camada de rede. O último fragmento pode ter menos
 * de 1024 bytes.
 */
public class Fragmento {

    public static final int TAMANHO_MAXIMO = 1024;

    private final int idTransferencia;
    private final int numeroFragmento;
    private final byte[] payload;

    public Fragmento(int idTransferencia, int numeroFragmento, byte[] payload) {
        if (idTransferencia < 0) {
            throw new IllegalArgumentException("idTransferencia não pode ser negativo");
        }
        if (numeroFragmento < 0) {
            throw new IllegalArgumentException("numeroFragmento não pode ser negativo");
        }
        if (payload == null) {
            throw new IllegalArgumentException("payload não pode ser nulo");
        }
        if (payload.length > TAMANHO_MAXIMO) {
            throw new IllegalArgumentException("payload excede o tamanho máximo de " + TAMANHO_MAXIMO + " bytes");
        }

        this.idTransferencia = idTransferencia;
        this.numeroFragmento = numeroFragmento;
        this.payload = Arrays.copyOf(payload, payload.length);
    }

    public int getIdTransferencia() {
        return idTransferencia;
    }

    public int getNumeroFragmento() {
        return numeroFragmento;
    }

    public byte[] getPayload() {
        return Arrays.copyOf(payload, payload.length);
    }

    public int getTamanho() {
        return payload.length;
    }
}
