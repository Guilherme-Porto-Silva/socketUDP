package cliente.componentes;

import cliente.UDP.ProtocoloUDP;
import cliente.UDP.RespostaUDP;
import cliente.fantoches.SocketTransferencia;
import cliente.interfaceGrafica.Monitor;
import comum.Comando;
import comum.Datagrama;
import comum.Fragmento;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Executa subir e baixar em segundo plano, fora da EDT do Swing.
 * Responsabilidades:
 *  - Fragmentar o arquivo em pedaços de 1024 bytes (MTU-safe);
 *  - Enviar/receber com ACK individual por fragmento;
 *  - Aplicar timeout de 2 s e retransmitir até 5 tentativas por fragmento;
 *  - Descartar fragmentos duplicados e bufferizar os fora de ordem;
 *  - Reportar progresso e resultado à interface exclusivamente via callback,
 *    sem tocar em componentes visuais.
 * A classe NÃO conhece Swing. Toda comunicação com a GUI passa pelo MONITOR.
 * Usa a API do ProtocoloUDP do pacote cliente (RespostaUDP com esgotado/temDados).
 */
public final class ThreadTransferencia extends Thread {

    private static final Logger LOGGER = Logger.getLogger(ThreadTransferencia.class.getName());

    /**
     * Tamanho do fragmento em bytes (1 KB, cabe no MTU Ethernet de 1500).
     */
    private static final int TAMANHO_FRAGMENTO = Fragmento.TAMANHO_MAXIMO; // 1024

    /**
     * Timeout de espera por resposta em milissegundos (2 s, adequado para LAN).
     */
    private static final int TIMEOUT_MS = 2000;

    /**
     * Limite de tentativas por fragmento/comando antes de abortar a transferência.
     */
    private static final int LIMITE_TENTATIVAS = 5;
    


//     estado

    private final Direcao DESCRICAO;

    private final Monitor MONITOR;

    private final InetSocketAddress SERVIDOR;

    private final SocketTransferencia MEIA_DE_TRANSFERENCIA;


//     Parâmetros de subir

    private final Path CAMINHO_ATE_ARQUIVO;


//     Parâmetros de baixar

    private final String NOME_ARQUIVO;

    private final Path DESTINO;


//     construção

    /**
     * Construtor para UPLOAD.
     *
     * @param protocolo      camada de rede do cliente (já aberta)
     * @param servidor       endereço do servidor (IP + porta)
     * @param caminhoArquivo arquivo local a ser enviado
     * @param nomeArquivo    nome do arquivo no servidor
     * @param monitor        callback para a interface gráfica
     */
    public ThreadTransferencia
    (ProtocoloUDP protocolo, InetSocketAddress servidor, Path caminhoArquivo, String nomeArquivo, Monitor monitor, SocketTransferencia meia) {

        super("ThreadTransferencia-UPLOAD");

        this.MEIA_DE_TRANSFERENCIA = meia;

        if (protocolo == null) throw new IllegalArgumentException("protocolo não pode ser nulo");

        if (servidor == null) throw new IllegalArgumentException("servidor não pode ser nulo");

        if (caminhoArquivo == null) throw new IllegalArgumentException("CAMINHO_ATE_ARQUIVO não pode ser nulo");

        if (nomeArquivo == null || nomeArquivo.trim().isEmpty()) throw new IllegalArgumentException("NOME_ARQUIVO não pode ser vazio");

        if (monitor == null) throw new IllegalArgumentException("monitor não pode ser nulo");

        DESCRICAO = Direcao.UPLOAD;

        SERVIDOR = servidor;

        CAMINHO_ATE_ARQUIVO = caminhoArquivo;

        NOME_ARQUIVO = nomeArquivo;

        DESTINO = null;

        MONITOR = monitor;
    }
    


    private static byte[] remontar (byte[][] buffer, long tamanhoEsperado) throws IOException {
    
        ByteArrayOutputStream baos = new ByteArrayOutputStream((int) tamanhoEsperado);
    
        for (byte[] frag : buffer) {
    
            if (frag == null) throw new IOException("Fragmento ausente na remontagem.");
            
            baos.write(frag);
        }
        
        byte[] resultado = baos.toByteArray();
        
        if (resultado.length != tamanhoEsperado) throw new IOException("Tamanho remontado (" + resultado.length + ") diverge do esperado (" + tamanhoEsperado + ").");
        
        return resultado;
    }



    /**
     * Construtor para DOWNLOAD.
     *
     * @param protocolo        camada de rede do cliente (já aberta)
     * @param servidor         endereço do servidor (IP + porta)
     * @param nomeArquivo      nome do arquivo a baixar
     * @param diretorioDestino pasta onde o arquivo será salvo
     * @param monitor          callback para a interface gráfica
     */
    public ThreadTransferencia
    (ProtocoloUDP protocolo, InetSocketAddress servidor, String nomeArquivo, Path diretorioDestino, Monitor monitor, SocketTransferencia socketTransferencia) {

        super("ThreadTransferencia-DOWNLOAD");

        this.MEIA_DE_TRANSFERENCIA = socketTransferencia;

        if (protocolo == null) throw new IllegalArgumentException("protocolo não pode ser nulo");

        if (servidor == null) throw new IllegalArgumentException("servidor não pode ser nulo");

        if (nomeArquivo == null || nomeArquivo.trim().isEmpty())

            throw new IllegalArgumentException("NOME_ARQUIVO não pode ser vazio");

        if (diretorioDestino == null) throw new IllegalArgumentException("diretorioDestino não pode ser nulo");

        if (monitor == null) throw new IllegalArgumentException("monitor não pode ser nulo");

        DESCRICAO = Direcao.DOWNLOAD;

        SERVIDOR = servidor;

        this.CAMINHO_ATE_ARQUIVO = null;

        this.NOME_ARQUIVO = nomeArquivo;

        DESTINO = diretorioDestino;

        MONITOR = monitor;
    }



    @Override public void run () {

        try {
            if (DESCRICAO == Direcao.UPLOAD) subir();

            else baixar();
        }

        catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            MONITOR.onErro("Transferência interrompida.");
        }

        catch (IOException e) {

            LOGGER.log(Level.SEVERE, "Erro de I/O na transferência", e);

            MONITOR.onErro("Erro de rede: " + e.getMessage());
        }

        catch (Exception e) {

            LOGGER.log(Level.SEVERE, "Falha inesperada na transferência", e);

            MONITOR.onErro("Falha inesperada: " + e.getMessage());
        }
    }



//     UPLOAD

    /**
     * Fluxo de subir:
     * 1. Lê o arquivo e calcula o hash SHA-256;
     * 2. Envia UPLOAD com metadados (nome, tamanho, nº fragmentos, hash);
     * 3. Aguarda ACK do servidor, que devolve o idTransferencia;
     * 4. Para cada fragmento, envia DATA e aguarda ACK individual,
     * retransmitindo em caso de timeout (até 5 tentativas);
     * 5. Notifica progresso a cada ACK recebido.
     */
    private void subir () throws IOException, InterruptedException {

//      Leitura e fragmentação

        byte[] conteudo = Files.readAllBytes(CAMINHO_ATE_ARQUIVO);

        String hash = Datagrama.calcularHashSha256(conteudo);

        int numeroFragmentos = calcularNumeroFragmentos(conteudo.length);

        MONITOR.onStatus("Preparando subir de " + NOME_ARQUIVO + "...");

//      Envia arquivo subido, com metadados

        Datagrama upload = Datagrama.subir(NOME_ARQUIVO, conteudo.length, numeroFragmentos, hash);

//      Aguarda ACK do arquivo subido (retorna o idTransferencia atribuído pelo servidor)

        int idTransferencia = aguardarAckComando(upload);

//      Envia fragmentos um a um, com ACK individual e retransmissão

        for (int i = 0; i < numeroFragmentos; i++) {

            byte[] payload = extrairPayload(conteudo, i);

            enviarFragmentoComRetentativa(idTransferencia, i, payload);

//         documentação do progresso

            double fracao = (double) (i + 1) / numeroFragmentos;

            MONITOR.onProgresso(fracao);

            MONITOR.onStatus("Enviando fragmento " + (i + 1) + " de " + numeroFragmentos);
        }

        MONITOR.onConcluido("Upload concluído: " + NOME_ARQUIVO);
    }



    /**
     * Envia um comando (UPLOAD ou DOWNLOAD) e aguarda ACK com retransmissão.
     * Retorna o idTransferencia informado pelo servidor no ACK.
     * Lança IOException se esgotar as tentativas ou se o servidor responder ERROR.
     */
    private int aguardarAckComando (Datagrama comando) throws IOException, InterruptedException {

        for (int tentativa = 1; tentativa <= LIMITE_TENTATIVAS; tentativa++) {

            MEIA_DE_TRANSFERENCIA.enviar(comando, SERVIDOR);

            RespostaUDP respostaUDP = MEIA_DE_TRANSFERENCIA.receber(TIMEOUT_MS);

            if (respostaUDP.esgotado()) {

                LOGGER.info("Timeout aguardando ACK (tentativa " + tentativa + "/" + LIMITE_TENTATIVAS + ")");

                continue;
            }

            if (respostaUDP.temDados()) {

                Datagrama respostaDatagrama = respostaUDP.datagrama();

                if (respostaDatagrama.getComando() == Comando.ERROR) throw new IOException("Servidor recusou: " + respostaDatagrama.getMensagemErro());

                if (respostaDatagrama.getComando() == Comando.ACK) return respostaDatagrama.getIdTransferencia();

//         Datagrama inesperado: descarta e tenta de novo

                LOGGER.warning("Resposta inesperada: " + respostaDatagrama.getComando());
            }
        }

        throw new IOException("Servidor não respondeu após " + LIMITE_TENTATIVAS + " tentativas.");
    }



//     DOWNLOAD

    /**
     * Fluxo de baixar:
     * 1. Envia DOWNLOAD com o nome do arquivo;
     * 2. Aguarda metadados (servidor responde com UPLOAD contendo nome, tamanho,
     * nº fragmentos e hash);
     * 3. Recebe os fragmentos DATA, enviando ACK individual a cada um;
     * fragmentos duplicados são descartados, fora de ordem são bufferizados;
     * 4. Ao completar, remonta o arquivo, confere o hash e salva no destino;
     * 5. Notifica progresso a cada fragmento confirmado.
     */
    private void baixar () throws IOException, InterruptedException {

//      Requisição

        MONITOR.onStatus("Solicitando baixar de " + NOME_ARQUIVO + "...");

        Datagrama download = Datagrama.baixar(NOME_ARQUIVO);

//      Aguarda metadados (servidor responde com UPLOAD contendo nome, tamanho, nº fragmentos, hash)

        MetadadosDownload meta = aguardarMetadadosDownload(download);

//         Buffer de fragmentos recebidos (indexado por numeroFragmento)

        byte[][] buffer = new byte[meta.numeroFragmentos()][];

        int recebidos = 0;

        while (recebidos < meta.numeroFragmentos()) {

            RespostaUDP r = MEIA_DE_TRANSFERENCIA.receber(TIMEOUT_MS);

            if (r.esgotado()) {

//              Timeout sem receber nada: não há como retransmitir do lado cliente

//              (quem retransmite é o servidor). Apenas seguimos aguardando.

                continue;
            }

            if (r.temDados()) {

                Datagrama resposta = r.datagrama();

                if (resposta.getComando() == Comando.ERROR) throw new IOException("Servidor reportou erro no baixar: " + resposta.getMensagemErro());

                if (resposta.getComando() == Comando.DATA) {

                    int numFrag = resposta.getNumeroFragmento();

                    int idTrans = resposta.getIdTransferencia();

//                  Valida se pertence a esta transferência

                    if (idTrans != meta.idTransferencia()) {

                        LOGGER.warning("DATA com idTransferencia inesperado: " + idTrans);

                        continue;
                    }

//                  Valida número do fragmento

                    if (numFrag < 0 || numFrag >= meta.numeroFragmentos()) {

                        LOGGER.warning("DATA com numeroFragmento fora do intervalo: " + numFrag);

                        continue;
                    }

//                  Descarta duplicados

                    if (buffer[numFrag] != null) {

                        LOGGER.fine("Fragmento duplicado descartado: " + numFrag);

                        continue;
                    }

//                  Bufferiza (fora de ordem ou não — a remontagem acontece no final)
                    
                    buffer[numFrag] = resposta.getPayload();
                    
                    recebidos++;

//                  Envia ACK individual

                    MEIA_DE_TRANSFERENCIA.enviar(Datagrama.ack(meta.idTransferencia(), numFrag), SERVIDOR);

//                  Progresso

                    double fracao = (double) recebidos / meta.numeroFragmentos();

                    MONITOR.onProgresso(fracao);

                    MONITOR.onStatus("Recebendo fragmento " + recebidos + " de " + meta.numeroFragmentos());
                }
            }
//         Datagrama malformado já lançou IOException no ProtocoloUDP.receber()
        }

//      Remonta o arquivo

        byte[] conteudo = remontar(buffer, meta.tamanhoArquivo());

//      Confere o hash
        if (!Datagrama.hashConfere(conteudo, meta.hash())) throw new IOException("Hash SHA-256 divergente após a remontagem. Arquivo pode estar corrompido.");

//      Salva no destino

        Path destino = DESTINO.resolve(NOME_ARQUIVO);

        Files.write(destino, conteudo);

        MONITOR.onConcluido("Download concluído: " + NOME_ARQUIVO);
    }



    /**
     * Aguarda o datagrama de metadados do baixar (UPLOAD do servidor).
     * Em caso de timeout, reenvia a requisição DOWNLOAD (até LIMITE_TENTATIVAS).
     */
    private MetadadosDownload aguardarMetadadosDownload (Datagrama download) throws IOException, InterruptedException {

        for (int tentativa = 1; tentativa <= LIMITE_TENTATIVAS; tentativa++) {

            MEIA_DE_TRANSFERENCIA.enviar(download, SERVIDOR);

            RespostaUDP r = MEIA_DE_TRANSFERENCIA.receber(TIMEOUT_MS);

            if (r.esgotado()) {

                LOGGER.info("Timeout aguardando metadados do baixar (tentativa " + tentativa + ")");

                continue;
            }

            if (r.temDados()) {

                Datagrama resp = r.datagrama();

                if (resp.getComando() == Comando.ERROR) throw new IOException("Servidor recusou baixar: " + resp.getMensagemErro());

                if (resp.getComando() == Comando.UPLOAD) {
//                         
//      O servidor reutiliza o layout de UPLOAD para enviar metadados
//                         
//      (nome, tamanho, nº fragmentos, hash). O idTransferencia será
//                         
//      confirmado nos datagramas DATA subsequentes.

                    return new MetadadosDownload(
                            resp.getIdTransferencia(),
                            resp.getNome(),
                            resp.getTamanhoArquivo(),
                            resp.getNumeroFragmentos(),
                            resp.getHash()
                    );
                }

                LOGGER.warning("Resposta inesperada ao DOWNLOAD: " + resp.getComando());
            }
        }

        throw new IOException("Servidor não respondeu ao DOWNLOAD após " + LIMITE_TENTATIVAS + " tentativas.");
    }



//     auxiliares

    /**
     * Envia um fragmento DATA e aguarda o ACK individual.
     * Retransmite em caso de timeout, até LIMITE_TENTATIVAS tentativas.
     * Lança IOException se esgotar as tentativas.
     */
    private void enviarFragmentoComRetentativa (int idTransferencia, int numeroFragmento, byte[] payload) throws IOException, InterruptedException {

        Datagrama data = Datagrama.dados(idTransferencia, numeroFragmento, payload);

        for (int tentativa = 1; tentativa <= LIMITE_TENTATIVAS; tentativa++) {

            MEIA_DE_TRANSFERENCIA.enviar(data, SERVIDOR);

            RespostaUDP r = MEIA_DE_TRANSFERENCIA.receber(TIMEOUT_MS);

            if (r.esgotado()) {

                LOGGER.info("Timeout aguardando ACK do fragmento " + numeroFragmento
                        + " (tentativa " + tentativa + "/" + LIMITE_TENTATIVAS + ")");

                continue; // retransmite
            }

            if (r.temDados()) {

                Datagrama resp = r.datagrama();

                if (resp.getComando() == Comando.ERROR) throw new IOException("Servidor recusou fragmento " + numeroFragmento + ": " + resp.getMensagemErro());

                if (resp.getComando() == Comando.ACK && resp.getIdTransferencia() == idTransferencia && resp.getNumeroFragmento() == numeroFragmento)

                    return; // ACK correto, fragmento confirmado

//      ACK de outro fragmento ou datagrama inesperado: descarta e tenta de novo

                LOGGER.fine("Resposta inesperada aguardando ACK do fragmento " + numeroFragmento + ": " + resp.getComando());
            }
        }

        throw new IOException("Fragmento " + numeroFragmento + " não confirmado após " + LIMITE_TENTATIVAS + " tentativas. Transferência abortada.");
    }



    /**
     * Calcula quantos fragmentos de 1024 bytes são necessários para o arquivo.
     * Arquivos vazios geram 0 fragmentos (não faz sentido enviar).
     */
    private static int calcularNumeroFragmentos (long tamanhoArquivo) {

        if (tamanhoArquivo <= 0) return 0;

        return (int) Math.ceil((double) tamanhoArquivo / TAMANHO_FRAGMENTO);
    }


    /**
     * Extrai o payload do i-ésimo fragmento a partir do conteúdo completo.
     */
    private static byte[] extrairPayload (byte[] conteudo, int indiceFragmento) {

        int inicio = indiceFragmento * TAMANHO_FRAGMENTO;

        int fim = Math.min(inicio + TAMANHO_FRAGMENTO, conteudo.length);

        byte[] payload = new byte[fim - inicio];

        System.arraycopy(conteudo, inicio, payload, 0, payload.length);

        return payload;
    }
}