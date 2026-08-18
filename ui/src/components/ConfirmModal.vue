<script setup>
// Replaces window.confirm() for the one destructive action the console has. A
// native confirm cannot say what the consequence is with any typography, and
// cannot make the dangerous button the heavy one.
import Modal from './Modal.vue'

defineProps({
  title: String,
  body: String,
  confirm: { type: String, default: 'Confirm' },
  danger: Boolean,
  busy: Boolean,
})
const emit = defineEmits(['close', 'confirm'])
</script>

<template>
  <Modal :title="title" :confirm="confirm" :danger="danger" :busy="busy"
         @close="emit('close')" @submit="emit('confirm')">
    <p class="text-[13.5px] text-fg-2 leading-relaxed -mt-1">{{ body }}</p>
  </Modal>
</template>
