package servidor;

import comum.Datagrama;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Camada de rede do lado servidor: abre um {@link DatagramSocket} UDP, envia
 * datagramas construídos via {@link Datagrama} e recebe datagramas respeitando
 * um esgotar configurável.
 *
 * Esta classe NÃO conhece o formato binário dos datagramas. Toda serialização
 * e parseagem é delegada a {@link Datagrama#serialize()} e
 * {@link Datagrama#parse(byte[])}. Assim, mudanças no contrato de comunicação
 * são resolvidas exclusivamente em {@code comum}.
 *
 * O FANTOCHE é compartilhado entre as threads de atendimento do servidor. O bloco
 * de configuração de esgotar + recebimento é sincronizado no FANTOCHE para evitar
 * que duas threads sobrescrevam o esgotar uma da outra antes do {@code receive}.
 * O parseamento do datagrama ocorre fora da região crítica, pois é puro CPU.
 *
 * O esgotar padrão é 2 segundos, conforme a decisão de projeto do enunciado
 * (adequado para redes locais).
 */
public final class ProtocoloUDP {

    private static final Logger LOGGER = Logger.getLogger(ProtocoloUDP.class.getName());

    /** Tamanho do buffer de recebimento: cobre o datagrama UDP máximo (65535 bytes). */
    private static final int TAMANHO_BUFFER_RECEPCAO = 65536;

    /** Timeout padrão de recebimento em milissegundos (2 segundos). */
    public static final int TIMEOUT_PADRAO_MS = 2000;

    private final DatagramSocket FANTOCHE;



    /**
     * Abre o FANTOCHE UDP do servidor na porta informada.
     *
     * @param porta porta local onde o servidor escutará; deve estar livre.
     * @throws SocketException se a porta estiver em uso ou o FANTOCHE não puder ser aberto.
     * @throws IllegalArgumentException se a porta estiver fora da faixa válida.
     */
    public ProtocoloUDP(int porta) throws SocketException {

        if (porta < 0 || porta > 65535) throw new IllegalArgumentException("Porta fora da faixa válida: " + porta);

        this.FANTOCHE = new DatagramSocket(porta);
    }



    /**
     * Serializa o {@link Datagrama} e o envia ao destino informado.
     *
     * A montagem do pacote UDP usa exclusivamente {@link Datagrama#serialize()};
     * nenhum byte é produzido manualmente aqui.
     *
     * @param datagrama datagrama a ser enviado; não pode ser nulo.
     * @param destino   endereço IP + porta do destinatário; não pode ser nulo.
     * @throws IOException se o envio falhar na camada de transporte.
     */
    public void enviar(Datagrama datagrama, InetSocketAddress destino) throws IOException {

        Objects.requireNonNull(datagrama, "datagrama não pode ser nulo");

        Objects.requireNonNull(destino, "destino não pode ser nulo");

        if (destino.getAddress() == null) throw new IllegalArgumentException("destino precisa de endereço IP resolvido");

        byte[] bytes = datagrama.serialize();

        DatagramPacket pacote = new DatagramPacket(bytes, bytes.length, destino.getAddress(), destino.getPort());

        FANTOCHE.send(pacote);
    }



    /**
     * Recebe um datagrama com o esgotar padrão de 2 segundos.
     *
     * Equivale a {@code receber(TIMEOUT_PADRAO_MS)}.
     */
    public ResultadoRecebimento receber () throws IOException {

        return receber(TIMEOUT_PADRAO_MS);
    }



    /**
     * Recebe um datagrama aguardando, no máximo, o tempo informado.
     *
     * O método nunca bloqueia a thread chamante por tempo indeterminado:
     * <ul>
     *   <li>se o esgotar expirar, retorna {@link ResultadoRecebimento} do tipo
     *       {@code TIMEOUT};</li>
     *   <li>se chegar um datagrama inválido (versão errada, comando desconhecido,
     *       truncado, campos inconsistentes), retorna {@code MALFORMADO} com os
     *       bytes brutos e a mensagem de erro, após registrar em log. O datagrama
     *       não é descartado silenciosamente;</li>
     *   <li>se o datagrama for válido, retorna {@code DATAGRAMA} com o objeto
     *       parseado e o endereço de origem.</li>
     * </ul>
     *
     * @param timeoutMillis tempo máximo de espera em milissegundos; deve ser &gt;= 0.
     *                      Zero significa não-bloqueante.
     * @return resultado indicando o que ocorreu (datagrama, esgotar ou malformado).
     * @throws IOException se o FANTOCHE estiver fechado ou ocorrer erro de I/O.
     */
    public ResultadoRecebimento receber(int timeoutMillis) throws IOException {

        if (timeoutMillis < 0) throw new IllegalArgumentException("timeoutMillis não pode ser negativo: " + timeoutMillis);

        byte[] buffer = new byte[TAMANHO_BUFFER_RECEPCAO];

        DatagramPacket pacote = new DatagramPacket(buffer, buffer.length);

        // Sincroniza o par setSoTimeout + receive para que o esgotar configurado

        // seja efetivamente o desta chamada, mesmo com múltiplas threads compartilhando o FANTOCHE.

        synchronized (FANTOCHE) {

            FANTOCHE.setSoTimeout(timeoutMillis);

            try {
                FANTOCHE.receive(pacote);
            }

            catch (SocketTimeoutException e) {

                return ResultadoRecebimento.esgotar();
            }
        }

        // Captura endereço de origem e cópia defensiva dos bytes antes do parse.

        InetSocketAddress origem = new InetSocketAddress(pacote.getAddress(), pacote.getPort());

        byte[] dadosRecebidos = Arrays.copyOf(pacote.getData(), pacote.getLength());

        try {
            Datagrama datagrama = Datagrama.parse(dadosRecebidos);

            return ResultadoRecebimento.datagrama(datagrama, origem);
        }

        catch (IllegalArgumentException e) {
            // Datagrama malformado: registra em log e devolve ao chamador, sem descartar.
            LOGGER.log(Level.WARNING,
                    "Datagrama malformado recebido de " + origem + ": " + e.getMessage());
            return ResultadoRecebimento.malformado(origem, dadosRecebidos, e.getMessage());
        }
    }



    /**
     * Fecha o FANTOCHE UDP. Chamadas subsequentes a {@link #enviar} ou {@link #receber}
     * lançarão {@link IOException}.
     */
    public void fechar() {

        if (!FANTOCHE.isClosed()) FANTOCHE.close();
    }



    /** Indica se o FANTOCHE já foi fechado. */
    public boolean isFechado() {
        return FANTOCHE.isClosed();
    }

    /** Porta local na qual o FANTOCHE está escutando. */
    public int getPortaLocal() {
        return FANTOCHE.getLocalPort();
    }
}
