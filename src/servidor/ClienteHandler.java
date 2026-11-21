package servidor;

import comum.Comando;
import comum.Datagrama;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * ClienteHandler — thread que atende os comandos de um cliente.
 * Atende:
 * - LIST: devolve a lista atual de arquivos do repositório.
 * - UPLOAD: recebe fragmentos DATA, confirma cada um, remonta fora de ordem e valida o hash.
 * - DOWNLOAD: envia fragmentos numerados e reenvia os que ficarem sem ACK (máximo 5 tentativas).
 * - UPDATE_LIST: disparado via Servidor quando o repositório muda.
 *
 * Existem dois tipos de atendimento, com a mesma lógica de comandos:
 * - cliente registrado (REGISTER): thread de vida longa, criada pelo Servidor, que atende os
 *   comandos enviados pelo socket principal do cliente;
 * - transferência avulsa: thread criada pelo Servidor quando um UPLOAD ou DOWNLOAD chega de um
 *   endereço sem registro, isto é, do socket dedicado de uma transferência do cliente. Atende
 *   uma única operação e termina.
 *
 * A thread nunca lê o socket UDP. O Servidor é o único leitor: ele entrega cada datagrama, por
 * {@link #entregar(Datagrama)}, à fila do atendimento do endereço de origem. Assim, vários
 * atendimentos convivem na mesma porta UDP sem disputar datagramas entre si.
 */
public class ClienteHandler implements Runnable {

    private static final int TAMANHO_FRAGMENTO = 1024;

    private static final int TIMEOUT_MS = 2000;

    private static final int MAX_TENTATIVAS = 5;

    private final ProtocoloUDP PROTOCOLO;

    private final InetSocketAddress ENDERECO_DO_CLIENTE;

    private final GerenciadorArquivos GERENCIADOR;

    private final LogServidor RELATORIO;

    private final Servidor SERVIDOR;

    private final String ID_CLIENTE;

    /** Verdadeiro para a transferência avulsa: a thread atende uma operação e termina. */
    private final boolean TRANSFERENCIA_UNICA;

    /** Datagramas do endereço deste atendimento, entregues pelo Servidor. */
    private final BlockingQueue<Datagrama> FILA = new LinkedBlockingQueue<>();

    private volatile boolean ativo = true;

    /** Metadados do download em andamento; reenviados se o cliente repetir o DOWNLOAD. */
    private Datagrama metadadosDoDownload;



    public ClienteHandler
      (ProtocoloUDP protocolo, InetSocketAddress enderecoCliente, GerenciadorArquivos gerenciador, LogServidor log, Servidor servidor, String idCliente, boolean transferenciaUnica) {

        PROTOCOLO = protocolo;

        ENDERECO_DO_CLIENTE = enderecoCliente;

        GERENCIADOR = gerenciador;

        RELATORIO = log;

        SERVIDOR = servidor;

        ID_CLIENTE = idCliente;

        TRANSFERENCIA_UNICA = transferenciaUnica;

        if (!TRANSFERENCIA_UNICA) RELATORIO.registrarConexao(origem(), "Cliente " + idCliente + " iniciado");
    }



    /** Recebe do Servidor um datagrama vindo do endereço deste atendimento. */
    public void entregar (Datagrama datagrama) {

        FILA.offer(datagrama);
    }



    @Override public void run () {

        try {

            if (TRANSFERENCIA_UNICA) {

                try { processarComSeguranca(receberDatagrama(TIMEOUT_MS)); }

                catch (SocketTimeoutException e) { RELATORIO.registrarTimeout(origem(), "Transferência sem comando inicial."); }
            }

            else {

                while (ativo) {

                    try { processarComSeguranca(receberDatagrama(TIMEOUT_MS)); }

                    catch (SocketTimeoutException e) { /* cliente ocioso: segue aguardando comandos */ }
                }
            }
        }

        catch (IOException e) {

            RELATORIO.registrarErro(origem(), "Atendimento interrompido: " + e.getMessage());
        }

        finally { encerrar(); }
    }



    /** Falha em um comando não derruba o atendimento: o erro é registrado e informado ao cliente. */
    private void processarComSeguranca (Datagrama datagrama) {

        try { processar(datagrama); }

        catch (ClienteAbortouException e) {

            RELATORIO.registrarErro(origem(), "Cliente abortou a operação: " + e.getMessage());
        }

        catch (IOException | RuntimeException e) {

            RELATORIO.registrarErro(origem(), "Falha ao processar " + datagrama.getComando() + ": " + e.getMessage());

            enviarErro("5000", "Falha interna ao processar " + datagrama.getComando());
        }
    }



    private void processar (Datagrama datagrama) throws IOException {

        Comando comando = datagrama.getComando();

        switch (comando) {

            case LIST -> processarLista();

            case UPLOAD -> processarUpload(datagrama);

            case DOWNLOAD -> baixar(datagrama);

            case ERROR -> RELATORIO.registrarErro(origem(), "Cliente reportou: " + datagrama.getMensagemErro());

            case ACK, DATA -> { /* confirmação ou fragmento atrasado de uma operação já encerrada: ignorado */ }

            default -> enviarErro("1001", "Comando nao suportado: " + comando);
        }
    }



    private void processarLista () throws IOException {

        String lista = SERVIDOR.listaSerializada();

        enviar(Datagrama.atualizarLista(lista));

        RELATORIO.registrar(origem(), "LIST", "OK", "Lista de arquivos enviada ao cliente");
    }



    private void processarUpload (Datagrama pedido) throws IOException {

        String nomeOriginal = pedido.getNome();

        long tamanho = pedido.getTamanhoArquivo();

        int totalFragmentos = pedido.getNumeroFragmentos();

        String hashInformado = pedido.getHash();

        if (nomeOriginal == null || nomeOriginal.isEmpty() || hashInformado == null || hashInformado.isEmpty()) {

            enviarErro("2001", "Campos obrigatorios ausentes no UPLOAD");

            return;
        }

        if (totalFragmentos < 0) {

            enviarErro("2002", "Numero de fragmentos invalido");

            return;
        }

        // Atribui um id de transferência único para esta operação.
        int idTransferencia = SERVIDOR.novoIdTransferencia();

        Map<Integer, byte[]> fragmentosRecebidos = new HashMap<>();

        // ACK inicial do UPLOAD: informa idTransferencia ao cliente.
        enviarAck(idTransferencia, 0);

        RELATORIO.registrarUpload(origem(), "INICIO", nomeOriginal + " (" + tamanho + " bytes em " + totalFragmentos + " fragmentos, id=" + idTransferencia + ")");

        int esgotamentosConsecutivos = 0;

        while (fragmentosRecebidos.size() < totalFragmentos) {

            Datagrama datagrama;

            try { datagrama = receberDatagrama(TIMEOUT_MS); }

            catch (SocketTimeoutException e) {

                esgotamentosConsecutivos++;

                RELATORIO.registrarTimeout(origem(), "UPLOAD de " + nomeOriginal + " aguardando fragmentos (" + esgotamentosConsecutivos + "/" + MAX_TENTATIVAS + ")");

                if (esgotamentosConsecutivos >= MAX_TENTATIVAS) {

                    enviarErro("2003", "Timeout aguardando fragmentos do upload");

                    RELATORIO.registrarUpload(origem(), "ERRO", "Abortado por timeout aguardando fragmentos de " + nomeOriginal);

                    return;
                }

                continue;
            }

            esgotamentosConsecutivos = 0;

            Comando comando = datagrama.getComando();

            if ( (comando == Comando.DATA) && (datagrama.getIdTransferencia() == idTransferencia) ) {

                int numero = datagrama.getNumeroFragmento();

                if (numero < 0 || numero >= totalFragmentos) {

                    enviarErro("2005", "Fragmento fora dos limites: " + numero);

                    continue;
                }

                byte[] payload = datagrama.getPayload();

                if (payload == null) payload = new byte[0];

                if (fragmentosRecebidos.containsKey(numero)) {

                    // Duplicado: descarta e reconfirma.
                    enviarAck(idTransferencia, numero);

                    continue;
                }

                fragmentosRecebidos.put(numero, payload);

                enviarAck(idTransferencia, numero);
            }

            else if (comando == Comando.UPLOAD) {

                // O cliente repetiu o pedido: o ACK inicial se perdeu.
                enviarAck(idTransferencia, 0);
            }

            else if (comando == Comando.ERROR) {

                RELATORIO.registrarUpload(origem(), "ERRO", "Cliente reportou: " + datagrama.getMensagemErro());

                return;
            }
        }

        // Remonta e valida.
        byte[] conteudo = remontar(fragmentosRecebidos, totalFragmentos, tamanho);

        if (conteudo == null) {

            enviarErro("2006", "Remontagem falhou: tamanho dos fragmentos inconsistente");

            RELATORIO.registrarUpload(origem(), "ERRO", "Remontagem falhou para " + nomeOriginal);

            return;
        }

        String hashCalculado = calcularHashSha256(conteudo);

        if (!hashCalculado.equalsIgnoreCase(hashInformado)) {

            enviarErro("2007", "Hash do arquivo nao confere");

            RELATORIO.registrarUpload(origem(), "ERRO", "Divergencia de hash para " + nomeOriginal);

            return;
        }

        String nomeFinal;

        try { nomeFinal = GERENCIADOR.salvarArquivo(nomeOriginal, conteudo); }

        catch (IOException e) {

            enviarErro("2008", "Falha ao gravar o arquivo no servidor");

            RELATORIO.registrarUpload(origem(), "ERRO", "Falha ao gravar " + nomeOriginal + ": " + e.getMessage());

            return;
        }

        RELATORIO.registrarUpload(origem(), "OK", nomeOriginal + " salvo como " + nomeFinal + " (" + conteudo.length + " bytes)");

        SERVIDOR.notificarAtualizacaoLista();

        // ACK final de conclusão (numeroFragmento=-1 sinaliza fim).
        Datagrama conclusao = Datagrama.ack(idTransferencia, -1);

        enviar(conclusao);

        if (TRANSFERENCIA_UNICA) aguardarRetransmissoesFinais(idTransferencia, conclusao);
    }



    /**
     * Mantém a transferência avulsa por mais um período de espera depois do ACK de conclusão.
     * Se o ACK do último fragmento ou o ACK de conclusão se perder, o cliente repete o último
     * DATA e o servidor reconfirma.
     */
    private void aguardarRetransmissoesFinais (int idTransferencia, Datagrama conclusao) {

        while (true) {

            Datagrama datagrama;

            try { datagrama = receberDatagrama(TIMEOUT_MS); }

            catch (IOException e) { return; }

            if ( (datagrama.getComando() == Comando.DATA) && (datagrama.getIdTransferencia() == idTransferencia) ) {

                enviarAck(idTransferencia, datagrama.getNumeroFragmento());

                enviar(conclusao);
            }
        }
    }



    private void baixar (Datagrama pedido) throws IOException {

        String nome = pedido.getNome();

        if (nome == null || nome.isEmpty()) {

            enviarErro("3001", "Nome de arquivo nao informado");

            return;
        }

        int idTransferencia = SERVIDOR.novoIdTransferencia();

        if (!GERENCIADOR.existeArquivo(nome)) {

            enviarErro("3002", "Arquivo inexistente: " + nome);

            RELATORIO.registrarDownload(origem(), "ERRO", "Arquivo inexistente: " + nome);

            return;
        }

        byte[] conteudo;

        try { conteudo = GERENCIADOR.lerArquivo(nome); }

        catch (IOException e) {

            enviarErro("3003", "Falha ao ler o arquivo: " + nome);

            RELATORIO.registrarDownload(origem(), "ERRO", "Falha ao ler " + nome + ": " + e.getMessage());

            return;
        }

        int totalFragmentos = (conteudo.length + TAMANHO_FRAGMENTO - 1) / TAMANHO_FRAGMENTO;

        String hash = calcularHashSha256(conteudo);

        // Envia metadados como UPLOAD (reutiliza o layout: nome, tamanho, nº fragmentos, hash, id da transferência).
        metadadosDoDownload = Datagrama.subir(nome, conteudo.length, totalFragmentos, hash)
                .comIdTransferencia(idTransferencia);

        enviar(metadadosDoDownload);

        RELATORIO.registrarDownload(origem(), "INICIO", nome + " (" + conteudo.length + " bytes em " + totalFragmentos + " fragmentos, id=" + idTransferencia + ")");

        for (int indice = 0; indice < totalFragmentos; indice++) {

            Datagrama data = Datagrama.dados(idTransferencia, indice, extrairFragmento(conteudo, indice));

            boolean confirmado = false;

            for (int tentativa = 1; tentativa <= MAX_TENTATIVAS && !confirmado; tentativa++) {

                enviar(data);

                confirmado = aguardarAck(idTransferencia, indice);

                if (!confirmado && tentativa < MAX_TENTATIVAS) {

                    RELATORIO.registrarRetransmissao(origem(), indice, "sem ACK, reenviando (tentativa " + (tentativa + 1) + "/" + MAX_TENTATIVAS + ")");
                }
            }

            if (!confirmado) {

                enviarErro("4002", "Cliente nao confirmou ACKs; transferencia abortada no fragmento " + indice);

                RELATORIO.registrarDownload(origem(), "ERRO", "Abortado apos " + MAX_TENTATIVAS + " tentativas no fragmento " + indice);

                return;
            }
        }

        // ACK final de conclusão.
        enviar(Datagrama.ack(idTransferencia, -1));

        RELATORIO.registrarDownload(origem(), "OK", nome + " enviado (" + conteudo.length + " bytes em " + totalFragmentos + " fragmentos)");
    }



    /**
     * Aguarda, por até TIMEOUT_MS, o ACK de um fragmento. Confirmações de outros fragmentos são
     * ignoradas. Se o cliente repetir o DOWNLOAD, os metadados se perderam e são reenviados.
     *
     * @return {@code true} se o ACK esperado chegou; {@code false} se o tempo esgotou.
     * @throws ClienteAbortouException se o cliente enviar ERROR.
     */
    private boolean aguardarAck (int idTransferencia, int fragmentoEsperado) throws IOException {

        long prazo = System.currentTimeMillis() + TIMEOUT_MS;

        while (true) {

            long restante = prazo - System.currentTimeMillis();

            if (restante <= 0) return false;

            Datagrama datagrama;

            try { datagrama = receberDatagrama((int) restante); }

            catch (SocketTimeoutException e) { return false; }

            Comando comando = datagrama.getComando();

            if ( (comando == Comando.ACK) && (datagrama.getIdTransferencia() == idTransferencia) && (datagrama.getNumeroFragmento() == fragmentoEsperado) ) return true;

            if ( (comando == Comando.DOWNLOAD) && (metadadosDoDownload != null) ) enviar(metadadosDoDownload);

            if (comando == Comando.ERROR) throw new ClienteAbortouException(datagrama.getMensagemErro());
        }
    }



    private byte[] extrairFragmento (byte[] conteudo, int indice) {

        int inicio = indice * TAMANHO_FRAGMENTO;

        int fim = Math.min(inicio + TAMANHO_FRAGMENTO, conteudo.length);

        return Arrays.copyOfRange(conteudo, inicio, fim);
    }



    private byte[] remontar (Map<Integer, byte[]> fragmentos, int totalFragmentos, long tamanhoEsperado) {

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        for (int i = 0; i < totalFragmentos; i++) {

            byte[] parte = fragmentos.get(i);

            if (parte == null) return null;

            buffer.write(parte, 0, parte.length);
        }

        byte[] completo = buffer.toByteArray();

        if (completo.length != tamanhoEsperado) return null;

        return completo;
    }



    private String calcularHashSha256 (byte[] dados) {

        try {

            MessageDigest digest = MessageDigest.getInstance("SHA-256");

            byte[] hash = digest.digest(dados);

            StringBuilder hex = new StringBuilder();

            for (byte b : hash) hex.append(String.format("%02x", b & 0xff));

            return hex.toString();
        }

        catch (NoSuchAlgorithmException e) {

            return "";
        }
    }



    private void enviarAck (int idTransferencia, int numeroFragmento) {

        enviar(Datagrama.ack(idTransferencia, numeroFragmento));
    }



    private void enviarErro (String codigo, String mensagem) {

        int codigoInt;

        try { codigoInt = Integer.parseInt(codigo); } catch (NumberFormatException e) { codigoInt = 0; }

        enviar(Datagrama.erro(codigoInt, mensagem));
    }



    private void enviar (Datagrama datagrama) {

        try { PROTOCOLO.enviar(datagrama, ENDERECO_DO_CLIENTE); }

        catch (IOException e) {

            RELATORIO.registrarErro(origem(), "Falha ao enviar " + datagrama.getComando() + ": " + e.getMessage());
        }
    }



    /**
     * Espera o próximo datagrama entregue pelo Servidor.
     *
     * @throws SocketTimeoutException se nenhum datagrama chegar em {@code timeoutMs}.
     */
    private Datagrama receberDatagrama (int timeoutMs) throws IOException {

        try {

            Datagrama datagrama = FILA.poll(Math.max(1, timeoutMs), TimeUnit.MILLISECONDS);

            if (datagrama == null) throw new SocketTimeoutException("Nenhum datagrama em " + timeoutMs + " ms");

            return datagrama;
        }

        catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            throw new IOException("Atendimento interrompido", e);
        }
    }



    private String origem () {

        return ENDERECO_DO_CLIENTE.getAddress().getHostAddress() + ":" + ENDERECO_DO_CLIENTE.getPort();
    }



    public void encerrar () {

        if (!ativo) return;

        ativo = false;

        if (!TRANSFERENCIA_UNICA) RELATORIO.registrarConexao(origem(), "Cliente " + ID_CLIENTE + " encerrado");

        SERVIDOR.removerAtendimento(ENDERECO_DO_CLIENTE, this);
    }



    /** O cliente enviou ERROR durante uma operação: ela termina sem nova resposta ao cliente. */
    private static final class ClienteAbortouException extends IOException {

        private static final long serialVersionUID = 1L;

        ClienteAbortouException (String mensagem) { super(mensagem); }
    }
}
