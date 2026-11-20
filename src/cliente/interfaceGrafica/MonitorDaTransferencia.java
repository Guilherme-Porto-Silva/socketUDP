package cliente.interfaceGrafica;

import javax.swing.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Monitor de uma transferência. Os métodos de Monitor chegam fora da EDT e apenas guardam
 * valores; o temporizador da EDT lê esses valores. O resultado final (sucesso, erro ou
 * cancelamento) vale uma única vez: eventos posteriores são ignorados.
 */
public class MonitorDaTransferencia implements Monitor {

    private final AtomicBoolean ENCERRADA = new AtomicBoolean(false);

    private final List<Runnable> RECURSOS = new ArrayList<>();

    private volatile Thread thread;

    volatile boolean cancelada;

    volatile double fracao = -1;

    volatile String status = "";

    @Override public void onProgresso (double valor) { if (!ENCERRADA.get()) fracao = valor; }

    @Override public void onStatus (String mensagem) { if (!ENCERRADA.get() && mensagem != null) status = mensagem; }

    @Override public void onConcluido (String mensagem) { encerrar(true, mensagem == null ? "Transferência concluída." : mensagem); }

    @Override public void onErro (String mensagem) { encerrar(false, mensagem == null ? "Falha na transferência." : mensagem); }



    boolean encerrada () { return ENCERRADA.get(); }



    /** Registra a thread de transferência. Devolve falso se a transferência já terminou (por exemplo, cancelada). */
    boolean vincular (Thread nova) {

        if (ENCERRADA.get()) return false;

        thread = nova;

        return true;
    }



    /** Registra um recurso de rede a fechar no fim. Se a transferência já terminou, fecha na hora. */
    void adicionarRecurso (Runnable fechar) {

        boolean fecharAgora;

        synchronized (this) {

            fecharAgora = ENCERRADA.get();

            if (!fecharAgora) RECURSOS.add(fechar);
        }

        if (fecharAgora) fechar.run();
    }



    /** Cancela a transferência: libera a interface na hora e fecha os sockets para desbloquear a thread. */
    void cancelar () {

        cancelada = true;

        Thread atual = thread;

        encerrar(false, "Transferência cancelada.");

        if (atual != null) atual.interrupt();
    }



    private void encerrar (boolean sucesso, String mensagem) {

        if (!ENCERRADA.compareAndSet(false, true)) return;

        fecharRecursos();

        SwingUtilities.invokeLater(() -> finalizarTransferencia(this, sucesso, mensagem));
    }



    private void finalizarTransferencia (MonitorDaTransferencia monitorDaTransferencia, boolean sucesso, String mensagem) {

    }


    private void fecharRecursos () {

        List<Runnable> copia;

        synchronized (this) {

            copia = new ArrayList<>(RECURSOS);

            RECURSOS.clear();
        }

        for (Runnable fechar : copia) {

            try {
                fechar.run();
            }

            catch (RuntimeException e) {

                System.out.println("Falha ao fechar recurso de rede: " + e);
            }
        }
    }
}